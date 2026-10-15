"""
Mock handler for parsing data associated with our plugin, one data file at a time (see parallelizer.py)
"""

from pathlib import Path
import uuid


def size_loader(data_file):
    """
    Generates a document of the following structure for the data file:
    {
        "name": <filename>
        "size": <filesize>
    }
    """
    yield {"_id": str(uuid.uuid4()), "name": Path(data_file).name, "size": Path(data_file).stat().st_size}
