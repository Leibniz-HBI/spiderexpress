"""Connector package.

At the moment we have two connectors:
- CSV, for reading network from two CSVs
- Telegram, for scraping public channels from the Telegram web-interface
"""

from .csv import csv_connector
