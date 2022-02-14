from abc import ABC, abstractclassmethod

class Connector(ABC):

    @abstractclassmethod
    def get_layer(self, node_names: str) -> tuple[pd.DataFrame, pd.DataFrame]:
        pass
    