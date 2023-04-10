class AutoHubValidateError(Exception):
    reason = None

    def __init__(self, reason, *args, **kwargs):
        self.reason = reason
        super().__init__(*args, **kwargs)


class AutoHubValidator:
    """
    This class aims to provide an easy way to customize validation logic for installing a hub from a release.
    """

    def __init__(self, auto_hub_feature):
        self.auto_hub_feature = auto_hub_feature

    def validate(self, force=False, **kwargs):
        """
        Check if the release is valid to install.
        If invalid, it should raise an AutoHubValidateError include any reason to stop the progress.
        """
        pass
