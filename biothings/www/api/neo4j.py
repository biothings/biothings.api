from biothings.settings import BiothingSettings
import requests

biothing_settings = BiothingSettings()

class Neo4jQuery(object):
    def __init__(self):
        pass

    def query(q, **kwargs):
        # very simple query wrapper
        res = requests.post(biothing_settings.neo4j_host, 
                            auth=(biothing_settings.neo4j_username, biothing_settings.neo4j_password),
                            data = {"query": q, "params": {}})
        if isinstance(res, requests.models.Response) and res.status_code == 200:
            return rrr.json()
        else:
            return {'success': False, 'message': 'Invalid query.'}
