def one_job_per_file(self):
    """
    Arguments of the parser for each upload job: one data file per job

    (the loader copies this function into the uploader's class: it imports what it needs itself)
    """
    from pathlib import Path

    return [(str(data_file),) for data_file in sorted(Path(self.data_folder).glob("*")) if data_file.is_file()]
