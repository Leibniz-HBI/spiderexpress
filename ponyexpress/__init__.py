from pathlib import Path
from .connectors import TelegramConnector

with Path('README.md').open() as file:
    __doc__ = file.read()
    