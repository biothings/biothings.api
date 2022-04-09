import pydoc
from functools import partial
from inspect import signature


method_directive = ".. py:method::"
data_directive = ".. py:data::"
template = """{directive} {name}{signature}

{docstring}
"""


def generate_command_documentations(filepath, commands):
    from biothings.hub import HubCommands

    assert isinstance(commands, (dict, HubCommands)), "commands must be a HubCommands instance, or dict"

    title = "biothings.hub.commands\n==============="
    command_docs = []

    for command_name, command_data in commands.items():
        func = None
        sign = None
        docstring = ""
        directive = data_directive

        if callable(command_data):
            func = command_data
        elif isinstance(command_data, dict):
            func = command_data.get("command")

        if func:
            if callable(func):
                directive = method_directive
                if isinstance(func, partial):
                    func = func.func
                sign = signature(func)
                docstring = pydoc.render_doc(func, title="%s", renderer=pydoc.plaintext)
            else:
                docstring = "This is a instance of type: {}".format(type(func))

        command_docs.append(template.format(
            directive=directive,
            name=command_name,
            signature=str(sign) if sign else "",
            docstring=docstring
        ))

    command_docs = '\n\n'.join(command_docs)
    doc = f"{title}\n\n{command_docs}"

    with open(filepath, mode="w") as f:
        f.write(doc)
