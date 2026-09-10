"""
Derive "_exists_" aliases for the object fields of a freshly built index.

Elasticsearch has no single posting for "this object is present". An exists
query on an object field is expanded into a disjunction over every leaf below
it, so `_exists_:gnomad_genome` on an object with 146 leaves costs roughly 146x
what a single-field exists query costs -- tens of seconds on a
billion-document index, burning CPU on every shard.

Very often one subfield is populated on every document that holds the object,
and querying that subfield instead is equivalent:

    _exists_:gnomad_genome    ->    _exists_:gnomad_genome.chrom

Which subfield (if any) qualifies depends on the data and can change from build
to build, so it is measured against the index rather than assumed. The result
is stored in the index's `_meta.exists_field_aliases`, where the web tier picks
it up automatically.

How far down the mapping to look is set by MAX_OBJECT_DEPTH below: nested
objects such as `gnomad_genome.hom` are separate queries from their parent and
need their own aliases.

The check is exact, not statistical. A subfield can only hold a value on
documents where its parent object is present, so subfield ⊆ object always
holds, and count(subfield) == count(object) therefore proves the two queries
match the identical set of documents.
"""

import logging
import time

logger = logging.getLogger(__name__)

# the key '_meta.exists_field_aliases' is written under
META_KEY = "exists_field_aliases"

# an object needs at least this many subfields before an alias is worth
# deriving: the payoff scales with the subfield count, while the scan costs one
# slow query per object field regardless.
DEFAULT_MIN_SUBFIELDS = 8

# How deep to look for object fields, counted from the root:
#     0     -> root level only            (gnomad_genome)
#     1     -> root plus one nested level (gnomad_genome, gnomad_genome.hom)
#     2     -> ... and so on
#     None  -> every level, however deep
#
# Each object field scanned costs one slow exists-on-object query, so raising
# this lengthens the post-index step. Depth 1 is the sweet spot for
# myvariant-shaped data: it covers the nested objects that show up in slow
# traffic ('gnomad_genome.hom', 'gnomad_exome.af') while keeping the scan to a
# manageable number of queries.
MAX_OBJECT_DEPTH = 0

# how many subfield counts to ask for in a single _msearch request
_COUNT_BATCH = 100


def _collect_subfields(properties, prefix=""):
    """
    Every queryable leaf below an object, at any depth, as dotted paths.

    Subfields that cannot be matched by an exists query (index: false) or that
    live under a disabled object are left out -- an alias must be something we
    can actually query.
    """
    subfields = []
    for name, spec in (properties or {}).items():
        if not isinstance(spec, dict) or spec.get("enabled") is False:
            continue
        path = f"{prefix}{name}"
        if "properties" in spec:
            subfields.extend(_collect_subfields(spec["properties"], f"{path}."))
        elif spec.get("index") is not False:
            subfields.append(path)
    return subfields


def _object_fields(properties, max_depth, prefix="", depth=0):
    """
    [(object_field, [subfield, ...]), ...] for every object field down to
    max_depth levels below the root. See MAX_OBJECT_DEPTH for the numbering.

    A parent and a nested child are both reported in their own right, since
    '_exists_:gnomad_genome' and '_exists_:gnomad_genome.hom' are separate
    queries needing separate aliases.
    """
    objects = []
    for name, spec in (properties or {}).items():
        if not isinstance(spec, dict) or spec.get("enabled") is False:
            continue
        if "properties" not in spec:
            continue  # a leaf: exists queries on it are already cheap
        path = f"{prefix}{name}"
        objects.append((path, _collect_subfields(spec["properties"], f"{path}.")))
        if max_depth is None or depth < max_depth:
            objects.extend(_object_fields(spec["properties"], max_depth, f"{path}.", depth + 1))
    return objects


def _objects_to_scan(mapping_properties, min_subfields, max_depth=MAX_OBJECT_DEPTH):
    """
    The object fields worth deriving an alias for, biggest first so the log
    stays useful if the step is interrupted -- those are the ones actually
    hurting.
    """
    objects = [
        (path, subfields)
        for path, subfields in _object_fields(mapping_properties, max_depth)
        if len(subfields) >= min_subfields
    ]
    return sorted(objects, key=lambda item: len(item[1]), reverse=True)


async def _count_subfields(client, index, subfields):
    """{subfield: doc count}, batched through _msearch."""
    counts = {}
    for start in range(0, len(subfields), _COUNT_BATCH):
        batch = subfields[start : start + _COUNT_BATCH]
        body = []
        for subfield in batch:
            body.append({"index": index})
            body.append({"query": {"exists": {"field": subfield}}, "size": 0, "track_total_hits": True})
        responses = (await client.msearch(body=body))["responses"]
        for subfield, response in zip(batch, responses):
            if "error" in response:
                logger.warning("Count failed for '%s': %s", subfield, response["error"])
                continue
            counts[subfield] = response["hits"]["total"]["value"]
    return counts


def _pick_alias(candidates):
    """
    Choose deterministically among equally valid subfields: shallowest first
    (a direct child beats a deeply nested one), then shortest, then
    alphabetical.
    """
    return min(candidates, key=lambda subfield: (subfield.count("."), len(subfield), subfield))


async def derive_exists_field_aliases(
    client,
    index,
    min_subfields=DEFAULT_MIN_SUBFIELDS,
    max_depth=MAX_OBJECT_DEPTH,
    logger=logger,
):
    """
    Return {object_field: equivalent_subfield} for one index.

    Costs one exists-on-object query per scanned object field -- the very query
    being optimized, so it is slow by construction. Meant to run once, against
    a new index, before it takes traffic. 'max_depth' controls how many nested
    levels are scanned and therefore how long that takes; see MAX_OBJECT_DEPTH.
    """
    mappings = await client.indices.get_mapping(index=index)
    # the index pattern normally resolves to one concrete index
    properties = next(iter(mappings.values()))["mappings"].get("properties", {})

    targets = _objects_to_scan(properties, min_subfields, max_depth)
    logger.info(
        "Deriving _exists_ aliases for %s object field(s) in '%s' (max_depth=%s)",
        len(targets),
        index,
        "all" if max_depth is None else max_depth,
    )

    aliases = {}
    for field, subfields in targets:
        started = time.time()
        counts = await _count_subfields(client, index, subfields)
        if not counts:
            continue

        # the exists-on-object query: the expensive one, and the reference the
        # subfield counts have to match
        response = await client.search(
            index=index, body={"query": {"exists": {"field": field}}, "size": 0, "track_total_hits": True}
        )
        total = response["hits"]["total"]["value"]
        elapsed = time.time() - started

        if any(count > total for count in counts.values()):
            # a subfield cannot outnumber its parent object; the mapping and
            # the data disagree, so trust neither and leave this field alone
            logger.warning("Skipping '%s': subfield count exceeds object count, mapping may be stale", field)
            continue

        matching = [subfield for subfield, count in counts.items() if count == total]
        if matching:
            aliases[field] = _pick_alias(matching)
            logger.info(
                "  %s -> %s (%s/%s subfields present in all %s docs, scanned in %.1fs)",
                field,
                aliases[field],
                len(matching),
                len(subfields),
                total,
                elapsed,
            )
        else:
            logger.info(
                "  %s -> no subfield present in all %s docs, left as is (%s subfields, scanned in %.1fs)",
                field,
                total,
                len(subfields),
                elapsed,
            )

    return aliases


async def store_exists_field_aliases(client, index, aliases, logger=logger):
    """
    Record the derived map in the index's `_meta`, preserving whatever else is
    already there.
    """
    mappings = await client.indices.get_mapping(index=index)
    meta = next(iter(mappings.values()))["mappings"].get("_meta", {})
    meta[META_KEY] = aliases
    await client.indices.put_mapping(index=index, body={"_meta": meta})
    logger.info("Stored %s _exists_ alias(es) in '%s' _meta", len(aliases), index)
