import gzip
from os import listdir
from os.path import isfile, join
from tempfile import TemporaryDirectory, mkstemp

from tornado.web import RequestHandler, StaticFileHandler
from tornado.template import Template

from biothings.utils.serializer import to_json


catalog = Template("""
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
""")

logfile = Template("""
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
""")


class DefaultCORSHeaderMixin:
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Methods", "GET,OPTIONS")
        self.set_header("Access-Control-Allow-Headers", "*")


class HubLogDirHandler(DefaultCORSHeaderMixin, RequestHandler):
    def initialize(self, path):
        self.path = path

    def get(self, filename):

        if not filename:
            logs = sorted([
                f for f in listdir(self.path)
                if isfile(join(self.path, f))
            ])
            if 'filter' in self.request.arguments:
                _f = self.get_argument('filter')
                logs = filter(lambda f: _f in f, logs)
            if 'json' in self.request.arguments:
                self.finish(to_json(list(logs)))
            else:
                self.finish(catalog.generate(logs=logs))
            return

        if not isfile(join(self.path, filename)):
            self.set_status(404)
            return

        def get_log_content(file_path, **kwargs):
            lines = None
            if file_path.endswith(".gz"):
                with gzip.open(file_path, "rb") as f:
                    lines = f.read().decode().splitlines()
            else:
                with open(file_path, 'r') as file:
                    lines = file.readlines()
            if lines and kwargs:
                _l = self.get_argument('lines')
                lines = lines[-int(_l):]
            return lines

        lines = get_log_content(join(self.path, filename), **self.request.arguments)
        self.finish(logfile.generate(
            name=filename,
            lines=lines
        ))


class HubLogFileHandler(DefaultCORSHeaderMixin, StaticFileHandler):

    def options(self, *args, **kwargs):
        self.set_status(204)

    async def get(self, path: str, include_body: bool = True) -> None:
        """If request path is a gz file, we will uncompress it first, then return get with the uncompress file path
        """

        self.path = self.parse_url_path(path)
        absolute_path = self.get_absolute_path(self.root, self.path)
        self.absolute_path = self.validate_absolute_path(self.root, absolute_path)
        if self.absolute_path is None:
            return

        if include_body and self.path.endswith(".gz"):
            download_file_name = self.path.replace(".gz", "")
            self.set_header("Content-Type", "text")
            self.set_header("Content-Disposition", f"attachment; filename={download_file_name}")
            with TemporaryDirectory(dir=self.root) as temp_dir:
                _, temp_file = mkstemp(dir=temp_dir)
                with gzip.open(self.absolute_path, "rb") as fread, open(temp_file, mode="w") as fwrite:
                    content = fread.read().decode()
                    fwrite.write(content)
                return await super().get(temp_file, include_body=True)

        return await super().get(path, include_body=include_body)
