def custom_jobs(self):
    """
    Return list of (`*arguments`) passed to self.load_data, in order. for each parallelized jobs. Ex: [(x,1),(y,2),(z,3)]

    Here, one job per downloaded file: the parser (load_data in parser.py) gets the path of a data file
    rather than the data folder. Not the hidden files, nor the archives kept once uncompressed (see
    "uncompress" in the manifest): to parse compressed files, remove their extension below. This function
    is copied into the plugin's uploader, without the rest of this module: import what it needs inside it
    """
    import os

    archives = (".zip", ".gz", ".bz2", ".xz", ".tar", ".tgz")
    return [
        (os.path.join(self.data_folder, name),)
        for name in sorted(os.listdir(self.data_folder))
        if os.path.isfile(os.path.join(self.data_folder, name))
        and not name.startswith(".")
        and not name.endswith(archives)
    ]
