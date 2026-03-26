"""
Mock handler for parsing data associated with our plugin
"""

from pathlib import Path
import uuid


def size_loader(data_folder):
    """
    Iterates over the data folder and generates documents of the following structure:
    {
        "name": <filename>
        "size": <filesize>
    }
    """
    for file_path_object in Path(data_folder).glob("**/*"):
        document = {
            "_id": str(uuid.uuid4()),
            "name": file_path_object.name,
            "size": Path(file_path_object).stat().st_size,
        }
        yield document
