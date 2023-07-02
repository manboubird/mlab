import glob
# import openai


import spacy
import json

from icecream import ic
from devtools import debug
from dataclasses import dataclass

from langchain.vectorstores.qdrant import Qdrant
from langchain.text_splitter import CharacterTextSplitter
# from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.embeddings import HuggingFaceEmbeddings

# from typing import List

# from qdrant_client import QdrantClient
# from qdrant_client.http.models import ScoredPoint

# import numpy as np
from sentence_transformers import SentenceTransformer


model = SentenceTransformer("sentence-transformers/paraphrase-distilroberta-base-v1")

# load GiNZA dictionary load
nlp: spacy.Language = spacy.load('ja_ginza', exclude=["tagger", "parser", "ner", "lemmatizer", "textcat", "custom"])

# qdrant_server = QdrantClient(path="./qdrant", collection_name="fashion")

# def get_vector(text: str) -> List[float]:
#     # vectorization
#     doc: spacy.tokens.doc.Doc = nlp(text)
#     return doc.vector


def get_vector(text: str):
    # return np.array(model.encode(text))
    return model.encode(text)


@dataclass
class DocEmbeddings():
    doc: list[str]
    embeddings: list


def get_embeddings():
    """Command on llm_exec"""
    ic("Start LLM app ...")

    docs = []
    embeddings = []

    # files = glob.glob("data/01_raw/data_runway/*")
    files = glob.glob("data/**/**/*")
    # ic(files)
    for file in files:

        # topic_name = os.path.splitext(os.path.basename(file))[0]
        topic_name = "fashion"
        # ic(topic_name)
        with open(file) as f:
            # content = f.read()
            content_dict = json.load(f)
        content = content_dict['textContent']
        # ic(content)

        text_splitter = CharacterTextSplitter(chunk_size=100, chunk_overlap=0)
        doc = text_splitter.create_documents(texts=[content], metadatas=[{"name": topic_name, "source": "wikipedia"}])

        # spacy ver or transformer.
        # vec = get_vector(content)

        docs.extend(doc)
        # debug(embeddings)
        # embeddings.extend(vec)
    # embeddings = OpenAIEmbeddings()
    embeddings = HuggingFaceEmbeddings()

    debug(embeddings)

    Qdrant.from_documents(docs, embeddings, path="./qdrant", collection_name="article")

    ic("End LLM app ...")
    return DocEmbeddings()
    # return DocEmbeddings( doc=docs embeddings= embeddings )
