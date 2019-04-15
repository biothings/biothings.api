import biothings_client
import copy
from itertools import islice, chain
import logging
import re
from biothings.hub.datatransform.datatransform import DataTransform, DataTransformEdge, IDStruct, nested_lookup
from biothings.utils.loggers import get_logger
from biothings import config as btconfig


class BiothingsAPIEdge(DataTransformEdge):
    """
    APIEdge - IDLookupEdge object for API calls
    """
    def __init__(self, lookup, field, weight=1):
        super().__init__()
        self.init_state()
        self.scope = lookup
        self.field = field
        self.weight = weight

    def init_state(self):
        self._state = {
            "client": None,
            "logger": None
        }

    @property
    def client(self):
        if not self._state["client"]:
            try:
                self.prepare_client()
            except Exception as e:
                # if accessed but not ready, then just ignore and return invalid value for a client
                return None
        return self._state["client"]

    def prepare_client(self):
        raise NotImplementedError("Define in subclass")

    def edge_lookup(self, keylookup_obj, id_strct):
        """
        Follow an edge given a key.

        This method uses the data in the edge_object
        to find one key to another key using an api.
        :param edge:
        :param key:
        :return:
        """
        qr = self._query_many(keylookup_obj, id_strct)
        new_id_strct = self._parse_querymany(keylookup_obj, qr, id_strct, self.field)
        return new_id_strct

    def _query_many(self, keylookup_obj, id_strct):
        """
        Call the biothings_client querymany function with a list of identifiers
        and output fields that will be returned.
        :param id_lst: list of identifiers to query
        :return:
        """
        if not isinstance(id_strct, IDStruct):
            raise TypeError("id_strct shouldb be of type IDStruct")
        id_lst = id_strct.id_lst
        return self.client.querymany(id_lst,
                                     scopes=self.scope,
                                     fields=self.field,
                                     as_generator=True,
                                     returnall=True,
                                     size=keylookup_obj.batch_size)

    def _parse_querymany(self, keylookup_obj, qr, id_strct, field):
        """
        Parse the querymany results from the biothings_client into a structure
        that will later be used for document key replacement.
        :param qr: querymany results
        :return:
        """
        self.logger.debug("QueryMany Structure:  {}".format(qr))
        qm_struct = IDStruct()
        for q in qr['out']:
            query = q['query']
            val = nested_lookup(q, field)
            if val:
                for (orig_id, curr_id) in id_strct:
                    if query == curr_id:
                        qm_struct.add(orig_id, val)
        return qm_struct

class MyChemInfoEdge(BiothingsAPIEdge):
    """
    The MyChemInfoEdge uses the MyChem.info API to convert identifiers.
    """

    def __init__(self, lookup, field, weight=1):
        """
        :param lookup: The field in the API to search with the input identifier.
        :type lookup: str
        :param field: The field in the API to convert to.
        :type field: str
        :param weight: Weights are used to prefer one path over another. The path with the lowest weight is preferred. The default weight is 1.
        :type weight: int
        """
        super().__init__(lookup, field, weight)

    def prepare_client(self):
        """
        Load the biothings_client for the class
        :return:
        """
        self._state["client"] = biothings_client.get_client('drug')
        self.logger.info("Registering biothings_client 'gene'")


class MyGeneInfoEdge(BiothingsAPIEdge):
    """
    The MyGeneInfoEdge uses the MyGene.info API to convert identifiers.
    """

    def __init__(self, lookup, field, weight=1):
        """
        :param lookup: The field in the API to search with the input identifier.
        :type lookup: str
        :param field: The field in the API to convert to.
        :type field: str
        :param weight: Weights are used to prefer one path over another. The path with the lowest weight is preferred. The default weight is 1.
        :type weight: int
        """
        super().__init__(lookup, field, weight)

    def prepare_client(self):
        """
        Load the biothings_client for the class
        :return:
        """
        self._state["client"] = biothings_client.get_client('gene')
        self.logger.info("Registering biothings_client 'drug'")

