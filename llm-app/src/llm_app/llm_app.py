#!/usr/bin/env python
# -*- coding: utf-8 -*-
import glob
import os
from dotenv import load_dotenv
from langchain.vectorstores.qdrant import Qdrant
from langchain.text_splitter import CharacterTextSplitter
from langchain.embeddings.openai import OpenAIEmbeddings

load_dotenv()

def main():
    print("test")
    docs = []
    
    files = glob.glob("data/01_raw/data_runway/*")
    for file in files:
        topic_name = os.path.splitext(os.path.basename(file))[0]
        print(topic_name)
        with open(file) as f:
            content = f.read()
        
        text_splitter = CharacterTextSplitter(chunk_size=100, chunk_overlap=0)
        print("test")
        doc = text_splitter.create_documents(texts=[content], metadatas=[{"name": topic_name, "source": "wikipedia"}])
        docs.extend(doc)
    
    embeddings = OpenAIEmbeddings()
    db = Qdrant.from_documents(docs, embeddings, path="./qdrant", collection_name="fashion")

if __name__ == '__main__':
    main()
