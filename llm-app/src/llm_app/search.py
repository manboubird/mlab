import pathlib
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer


class NeuralSearcher:

    def __init__(self, collection_name):
        self.collection_name = collection_name
        self.model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')
        self.qdrant_client = QdrantClient(path=str(pathlib.Path(__file__).parent.parent.absolute() / "qdrant"))

    def search(self, text: str):
        vector = self.model.encode(text).tolist()
        search_result = self.qdrant_client.search(
            collection_name=self.collection_name,
            query_vector=vector,
            query_filter=None,
            # top=5
        )
        payloads = [hit.payload for hit in search_result]
        return payloads
