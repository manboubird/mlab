#!/usr/bin/env python
# -*- coding: utf-8 -*-
import glob
# import openai


import spacy
from icecream import ic
from devtools import debug

from dotenv import load_dotenv
from langchain.vectorstores.qdrant import Qdrant
from langchain.text_splitter import CharacterTextSplitter
# from langchain.embeddings.openai import OpenAIEmbeddings
from langchain.embeddings import HuggingFaceEmbeddings

from typing import List

from qdrant_client import QdrantClient
from qdrant_client.http.models import ScoredPoint

import numpy as np
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("sentence-transformers/paraphrase-distilroberta-base-v1")


load_dotenv()


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


def main():
    ic("Start LLM app ...")
    docs = []
    # embeddings = []

    files = glob.glob("data/01_raw/data_runway/*")
    for file in files:

        # topic_name = os.path.splitext(os.path.basename(file))[0]
        topic_name = "fashion"
        ic(topic_name)
        with open(file) as f:
            content = f.read()
        
        text_splitter = CharacterTextSplitter(chunk_size=100, chunk_overlap=0)
        doc = text_splitter.create_documents(texts=[content], metadatas=[{"name": topic_name, "source": "wikipedia"}])

        # spacy ver or transformer.
        # vec = get_vector(content)

        docs.extend(doc)
        # embeddings.extend(vec)
    
    # embeddings = OpenAIEmbeddings()
    embeddings = HuggingFaceEmbeddings()
    debug(embeddings)

    db = Qdrant.from_documents(docs, embeddings, path="./qdrant", collection_name="fashion")

    ic("End LLM app ...")

if __name__ == '__main__':
    main()
