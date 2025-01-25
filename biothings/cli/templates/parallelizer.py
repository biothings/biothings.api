def custom_jobs(self):
    """
    Return list of (`*arguments`) passed to self.load_data, in order. for each parallelized jobs. Ex: [(x,1),(y,2),(z,3)]
    """
    return [(f,) for f in [1, 2, 3]]
