import sys
from contextlib import ContextDecorator
from io import StringIO


class RedirectStdStreams(ContextDecorator):
    def __init__(self, stdout=None, stderr=None):
        self._stdout = stdout or StringIO()
        self._stderr = stderr or StringIO()

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush()
        self.old_stderr.flush()
        sys.stdout, sys.stderr = self._stdout, self._stderr
        return self

    def __exit__(self, *exc):
        self._stdout.flush()
        self._stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr

    def get_std_contents(self):
        self._stdout.seek(0)
        self._stderr.seek(0)
        stdout_content = "".join([line for line in self._stdout.readlines()])
        stderr_content = "".join([line for line in self._stderr.readlines()])
        return {
            "stdout": stdout_content,
            "stderr": stderr_content,
        }
