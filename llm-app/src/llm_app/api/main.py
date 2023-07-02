#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from fastapi import FastAPI

from ..llm import get_embeddings, DocEmbeddings
from ..search import NeuralSearcher

app = FastAPI()


@app.get("/predict")
async def predict():
    return {"message": "predict"}


@app.get("/embeddings")
async def embeddings():
    e: DocEmbeddings = get_embeddings()
    return {"doc": e.docs[0]}


searcher = NeuralSearcher(collection_name='fashion')


@app.get("/search")
def search(q: str):
    return {
        "result": searcher.search(text=q)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000, reload=True)
