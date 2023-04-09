import gzip
import os
from os import listdir
from os.path import isdir, isfile, join
from tempfile import TemporaryDirectory, mkstemp

from tornado.template import Template
from tornado.web import RequestHandler, StaticFileHandler

from biothings.utils.serializer import to_json

catalog = Template(
    """
    <html>
        <head>
            <title> Biothings Hub Logs </title>
        </head>
        <body>
            <h1> Logs </h1>
            <ul>
            {% for log in logs %}
                <li>
                    <a href={{"./" + log }}>
                        {{ log[:-4] if log.endswith('.log') else log }}
                    </a>
                </li>
            {% end %}
            </ul>
        </body>
    </html>
"""
)

logfile = Template(
    """
    <html>
        <head>
            <title> {{ "Log " + name }} </title>
        </head>
        <body>
            <h1> {{ name }} </h1>
            <p>
                {% for line in lines %}
                    {{ line }} </br>
                {% end %}
            </p>
        </body>
    </html>
"""
)


def get_log_content(file_path, **kwargs):
    lines = None
    if file_path.endswith(".gz"):
        with gzip.open(file_path, "rb") as f:
            lines = f.read().decode().splitlines()
    else:
        with open(file_path, "r") as file:
            lines = file.readlines()

    if lines and "lines" in kwargs:
        cap_lines = kwargs["lines"]
        cap_lines = cap_lines[0] if isinstance(cap_lines, list) else cap_lines
        try:
            cap_lines = int(cap_lines)
            lines = lines[-cap_lines:]
            if len(lines) == cap_lines:
                lines.append(f"\n***Logs were capped at {cap_lines} lines***")
        except Exception:
            pass
    return lines


class DefaultCORSHeaderMixin:
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Methods", "GET,OPTIONS")
        self.set_header("Access-Control-Allow-Headers", "*")


class HubLogDirHandler(DefaultCORSHeaderMixin, RequestHandler):
    def initialize(self, path):
        self.path = path

    def get(self, filename):
        fullname = join(self.path, filename)
        if isdir(fullname):
            logs = [f"{f}/" if isdir(join(fullname, f)) else f for f in listdir(fullname)]

            if "filter" in self.request.arguments:
                filters = self.get_argument("filter") or ""
                filters = filters.split(",")
                logs = set([f for keyword in filters for f in logs if keyword in f])

            logs = sorted(logs)

            if "json" in self.request.arguments:
                self.finish(to_json(list(logs)))
            else:
                self.finish(catalog.generate(logs=logs))
            return

        if not isfile(fullname):
            self.set_status(404)
            return

        lines = get_log_content(fullname, **self.request.arguments)
        self.finish(logfile.generate(name=filename, lines=lines))


class HubLogFileHandler(DefaultCORSHeaderMixin, StaticFileHandler):
    def options(self, *args, **kwargs):
        self.set_status(204)

    async def get(self, path: str, include_body: bool = True) -> None:
        """If request path is a gz file, we will uncompress it first, then return get with the uncompress file path"""

        self.path = self.parse_url_path(path)
        absolute_path = self.get_absolute_path(self.root, self.path)
        self.absolute_path = self.validate_absolute_path(self.root, absolute_path)
        if self.absolute_path is None:
            return

        if include_body:
            download_file_name = self.path.replace(".gz", "")
            self.set_header("Content-Type", "text")
            self.set_header("Content-Disposition", f"attachment; filename={download_file_name}")
            with TemporaryDirectory(dir=self.root) as temp_dir:
                _, temp_file = mkstemp(dir=temp_dir)
                lines = get_log_content(self.absolute_path, **self.request.arguments)
                lines = "\n".join(lines)
                with open(temp_file, mode="w") as fwrite:
                    fwrite.write(lines)
                return await super().get(temp_file, include_body=True)

        return await super().get(path, include_body=include_body)
