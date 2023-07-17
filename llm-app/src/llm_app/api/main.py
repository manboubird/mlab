#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from fastapi import FastAPI, Response

from ..llm import get_embeddings, DocEmbeddings
from ..search import NeuralSearcher

app = FastAPI()


@app.get("/predict")
async def predict(response: Response):
    # performace measure with Server-Timing
    #  Server Timing, https://w3c.github.io/server-timing/#examples
    response.headers["Server-Timing"] = ('sql-1;desc="MySQL lookup Server";dur=100, '
                                         'sql-2;dur=900;desc="MySQL shard Server #1", '
                                         'fs;dur=600;desc="FileSystem", '
                                         'cache;dur=300;desc="Cache", '
                                         'other;dur=200;desc="Database Write", '
                                         'other;dur=110;desc="Database Read", '
                                         'cpu;dur=1230;desc="Total CPU"')

    # response.headers["Server-Timing"] = ('miss, db;dur=53, app;dur=47.4, '
    #                                      'customView, dc;desc=atl, '
    #                                      'cache;desc="Cache Read";dur=23.2, '
    #                                      'Prerender;dur=1;desc="Headless render time (ms)", '
    #                                      'total;dur=124.6')

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
