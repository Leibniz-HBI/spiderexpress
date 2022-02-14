from abc import ABC, abstractclassmethod

class Strategy(ABC):

    @abstractclassmethod
    def get_sample(self, nodes, edges, visited_nodes) -> list[str]:
        pass
