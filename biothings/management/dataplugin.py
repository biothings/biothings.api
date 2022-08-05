import os
import pathlib
from shutil import copytree
from typing import Optional

import tornado.template
import typer

app = typer.Typer()


@app.command('create')
def create_data_plugin(
    name: Optional[str] = typer.Option(  # NOQA: B008
        default='',
        help="Data plugin name",
        prompt="What's your data plugin name?",
    ),
    multi_uploaders: bool = typer.Option(  # NOQA: B008
        False, "--multi-uploaders",
        help="Add this option if you want to create multiple uploaders"
    ),
    parallelizer: bool = typer.Option(  # NOQA: B008
        False, "--parallelizer", help="Using parallelizer or not? Default: No"
    ),
):
    workspace_dir = pathlib.Path().resolve()
    biothing_source_dir = pathlib.Path(__file__).parent.parent.resolve()
    template_dir = os.path.join(biothing_source_dir, 'hub', 'dataplugin', 'templates')
    plugin_dir = os.path.join(workspace_dir, name)
    if os.path.isdir(plugin_dir):
        print("Data plugin with the same name is already exists, please remove it before create")
        return exit(1)
    copytree(template_dir, plugin_dir)
    # create manifest file
    loader = tornado.template.Loader(plugin_dir)
    parsed_template = (
        loader.load('manifest.yaml.tpl')
        .generate(multi_uploaders=multi_uploaders, parallelizer=parallelizer)
        .decode()
    )
    manifest_file_path = os.path.join(workspace_dir, name, 'manifest.yaml')
    with open(manifest_file_path, "w") as fh:
        fh.write(parsed_template)

    # remove manifest template
    os.unlink(f"{plugin_dir}/manifest.yaml.tpl")
    if not parallelizer:
        os.unlink(f"{plugin_dir}/parallelizer.py")
    print(f"Successful create data plugin template at: \n {plugin_dir}")
