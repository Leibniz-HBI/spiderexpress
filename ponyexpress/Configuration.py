from pathlib import Path
from loguru import logger as log
import yaml

class Configuration(yaml.YAMLObject):

    _yaml_tag = '!telegramspider:Configuration'

    def __init__(
        self,
        seeds: list[str] or None,
        seed_file: str or None,
        project_name: str = 'spider',
        db_url: str = 'sqlite:///{project_name}.sqlite',
        edge_table_name: str = 'edge_list',
        node_table_name: str = 'node_list',
        strategy: str = 'spikyball',
        max_iteration: int = 10000,
        batch_size: int = 150,
    ) -> None:
        if (seeds is None and seed_file is None):
            raise
        if seed_file is not None:
            seed_file_path = Path(seed_file)
            self.seed_file = seed_file_path
            if not seed_file_path.exists():
                raise
            if seeds is not None:
                log.warn(f'overriding configured seeds with seeds from file: {seed_file}')
                self.seeds = None
        if strategy not in ['spikyball']:
            raise
        self.project_name = project_name
        self.db_url = db_url
        self.edge_table_name = edge_table_name
        self.node_table_name = node_table_name
        self.max_iteration = max_iteration
        self.batch_size = batch_size
