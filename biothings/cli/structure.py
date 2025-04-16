import pathlib

CLI_DIRECTORY = pathlib.Path(__file__).resolve().absolute().parent
TEMPLATE_DIRECTORY = pathlib.Path(CLI_DIRECTORY).joinpath("templates")
