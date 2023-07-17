#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Any

from fastapi import Depends, FastAPI, Response, Query
from pydantic import BaseModel, Field

from ..llm import get_embeddings, DocEmbeddings
from ..search import NeuralSearcher

app = FastAPI()


class PredictIn(BaseModel):
    name: str = Field(Query(default="future", description="Input name"))


class PredictOut(BaseModel):
    message: str


@app.get("/predict", response_model=PredictOut)
async def predict(response: Response, query: PredictIn = Depends()) -> Any:
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

    return {"message": f"Predict {query.name}!"}


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
