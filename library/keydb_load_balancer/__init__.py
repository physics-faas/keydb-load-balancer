"""
.. include:: ./documentation.md
"""

from .server import Server


_version_ = '0.0.1'

_all_ = [
  "Server"
]

# specify autocompletion in IPython
# see comment: https://github.com/ska-telescope/katpoint/commit/ed7e8b9e389ee035073c62c2394975fe71031f88
# _dir_ docs (Python 3.7!): https://docs.python.org/3.7/library/functions.html#dir


def __dir__():
    """IPython tab completion seems to respect this."""
    return _all_ + [
        "__all__",
        "__builtins__",
        "__cached__",
        "__doc__",
        "__file__",
        "__loader__",
        "__name__",
        "__package__",
        "__path__",
        "__spec__",
        "__version__",
    ]