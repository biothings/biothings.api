from os import listdir
from os.path import isfile, join

from tornado.web import RequestHandler, StaticFileHandler
from tornado.template import Template

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

class HubLogDirHandler(RequestHandler):

    def initialize(self, path):
        self.path = path

    def get(self, filename):

        if not filename:
            logs = sorted([
                f for f in listdir(self.path)
                if isfile(join(self.path, f))
            ])
            self.finish(catalog.generate(logs=logs))
            return

        if not isfile(join(self.path, filename)):
            self.set_status(404)
            return

        with open(join(self.path, filename), 'r') as file:
            self.finish(logfile.generate(
                name=filename,
                lines=file.read().split('\n')
            ))

class HubLogFileHandler(StaticFileHandler):

    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Methods", "GET,OPTIONS")
        self.set_header("Access-Control-Allow-Headers", "*")

    def options(self, *args, **kwargs):
        self.set_status(204)
