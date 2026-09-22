"""
Derive '_exists_:<object field>' aliases for a freshly built index.

An exists query on an object field expands into a disjunction over every
leaf below it, which is expensive on wide objects. When one subfield is
populated on every document that has the object, querying that subfield
instead returns the identical result set for a fraction of the cost. This
module measures which subfield (if any) qualifies and writes the result to
the index's `_meta.exists_field_aliases`, where the web tier applies it.

Correctness rests on subfield ⊆ object: a subfield can only have a value on
documents where its parent exists, so count(subfield) == count(object)
proves the two queries match the same documents.
"""

import logging
import time

logger = logging.getLogger(__name__)

META_KEY = "exists_field_aliases"

# minimum subfield count worth scanning for; payoff scales with subfield
# count but the scan cost per object field is fixed.
DEFAULT_MIN_SUBFIELDS = 8

# how many nested levels to scan, counted from the root (0 = root only,
# None = unlimited). Each level costs one slow query per object field found
# at that level. 1 covers the nested objects seen in slow-query logs.
MAX_OBJECT_DEPTH = 1

# subfield counts requested per _msearch batch
_COUNT_BATCH = 100


def _collect_subfields(properties, prefix=""):
    """Dotted paths of every queryable leaf below an object (skips unindexed/disabled fields)."""
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
    max_depth levels below the root. A parent and its nested child are both
    reported, since they need separate aliases.
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
    """Object fields worth deriving an alias for, largest first."""
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
    """Deterministic tie-break among valid subfields: shallowest, then shortest, then alphabetical."""
    return min(candidates, key=lambda subfield: (subfield.count("."), len(subfield), subfield))


async def _derive_one_alias(client, index, field, subfields, logger):
    """Measure one object field against its subfields; return the alias to use, or None. Logs the outcome."""
    started = time.time()
    counts = await _count_subfields(client, index, subfields)
    if not counts:
        return None

    # the expensive reference query the subfield counts must match
    response = await client.search(
        index=index, body={"query": {"exists": {"field": field}}, "size": 0, "track_total_hits": True}
    )
    total = response["hits"]["total"]["value"]
    elapsed = time.time() - started

    if any(count > total for count in counts.values()):
        # a subfield can't outnumber its parent; mapping/data disagree, so skip
        logger.warning("Skipping '%s': subfield count exceeds object count, mapping may be stale", field)
        return None

    matching = [subfield for subfield, count in counts.items() if count == total]
    if not matching:
        logger.info(
            "  %s -> no subfield present in all %s docs, left as is (%s subfields, scanned in %.1fs)",
            field,
            total,
            len(subfields),
            elapsed,
        )
        return None

    alias = _pick_alias(matching)
    logger.info(
        "  %s -> %s (%s/%s subfields present in all %s docs, scanned in %.1fs)",
        field,
        alias,
        len(matching),
        len(subfields),
        total,
        elapsed,
    )
    return alias


async def derive_exists_field_aliases(
    client,
    index,
    min_subfields=DEFAULT_MIN_SUBFIELDS,
    max_depth=MAX_OBJECT_DEPTH,
    logger=logger,
):
    """
    Return {object_field: equivalent_subfield} for one index. Costs one
    slow exists-on-object query per scanned field; meant to run once against
    a new index before it takes traffic.
    """
    mappings = await client.indices.get_mapping(index=index)
    # the index pattern resolves to one concrete index here
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
        alias = await _derive_one_alias(client, index, field, subfields, logger)
        if alias:
            aliases[field] = alias

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
