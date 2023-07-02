import glob
# import openai
import pathlib


import spacy
import json

from icecream import ic
# from devtools import debug
from dataclasses import dataclass

from langchain.vectorstores.qdrant import Qdrant
from langchain.text_splitter import CharacterTextSplitter
# from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.embeddings import HuggingFaceEmbeddings

# from typing import List

from qdrant_client import QdrantClient
from qdrant_client.http import models

import numpy as np
from sentence_transformers import SentenceTransformer


# load GiNZA dictionary load
nlp: spacy.Language = spacy.load('ja_ginza', exclude=["tagger", "parser", "ner", "lemmatizer", "textcat", "custom"])

QDRANT_ROOT = str(pathlib.Path(__file__).parent.parent.absolute() / "qdrant2")

qdrant_client = QdrantClient(path=QDRANT_ROOT, collection_name="fashion")

# def get_vector(text: str) -> List[float]:
#     # vectorization
#     doc: spacy.tokens.doc.Doc = nlp(text)
#     return doc.vector


model_name = "oshizo/sbert-jsnli-luke-japanese-base-lite"
# model_name = "sentence-transformers/paraphrase-distilroberta-base-v1"
# model_name = 'all-MiniLM-L6-v2'
# model = SentenceTransformer(model_name, device="cpu")


def get_vector(text: str):
    return np.array(models.encode(text))
    # return model.encode(text)


@dataclass
class DocEmbeddings():
    doc: list[str]
    embeddings: list


def recreate_collection(client: QdrantClient, name: str, vector_size: int) -> None:
    client.recreate_collection(
        collection_name=name,
        vectors_config=models.VectorParams(
            size=vector_size,
            distance=models.Distance.COSINE
        )
    )


def get_embeddings():
    """Command on llm_exec"""
    ic("Start LLM app ...")

    docs = []
    contents = []

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

        docs.extend(doc)

        # spacy ver or transformer.
        # vec = get_vector(content)
        # vec = model.encode(content)
        contents.extend(content)

        # debug(embeddings)


    # embeddings = OpenAIEmbeddings()
    embeddings = HuggingFaceEmbeddings(
        model_name=model_name
    )
    contents_results = embeddings.embed_documents(contents)

    collection_name = "runway"

    # recreate_collection(
    #     client=qdrant_client,
    #     name=collection_name,
    #     vector_size=len(contents_results[0])
    # )

    ic(embeddings)

    Qdrant.from_documents(docs, embeddings, path=QDRANT_ROOT, collection_name=collection_name)

    ic("End LLM app ...")
    # return DocEmbeddings(doc=docs, embeddings=embeddings)
    # return DocEmbeddings()
