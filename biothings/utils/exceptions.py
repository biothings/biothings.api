import json


class RepositoryVerificationFailed(Exception):
    def __str__(self):
        return json.dumps(self.args)
