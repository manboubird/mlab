#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import json

from pyinstrument import Profiler
from pyinstrument.renderers import JSONRenderer

from dataclasses import dataclass
from typing import Any

from devtools import debug

from fastapi import Depends, FastAPI, Response, Query
from pydantic import BaseModel, Field

from ..llm import get_embeddings, DocEmbeddings
from ..search import NeuralSearcher

app = FastAPI()


class PredictIn(BaseModel):
    name: str = Field(Query(default="future", description="Input name"))


class PredictOut(BaseModel):
    message: str


@dataclass
class ServerTimingEntry:
    label: str = 'None'
    desc: str = 'None'
    dur: int = 0

    def __str__(self):
        return '{};dur={};desc="{}"'.format(self.label, self.dur, self.desc)


@dataclass
class ServerTiming:
    cpu_time: ServerTimingEntry
    wall_clock_time: ServerTimingEntry

    def __post_init__(self):
        self.cpu_time = ServerTimingEntry(label='func_cpu_time', dur=self.cpu_time['dur'], desc="Func call cpu time")
        self.wall_clcok_time = ServerTimingEntry(label='func_wall_clock_time', dur=self.wall_clock_time['dur'], desc="Func call wall clock time")

    def __str__(self):
        return ','.join([str(self.cpu_time), str(self.wall_clcok_time)])


@app.get("/predict", response_model=PredictOut)
def predict(response: Response, query: PredictIn = Depends()) -> Any:

    profiler = Profiler()
    profiler.start()

    time.sleep(1)

    profiler.stop()
    jd = json.loads(profiler.output(JSONRenderer(show_all=False, timeline=False)))
    # debug(jd)

    # performace measure with Server-Timing
    #  Server Timing, https://w3c.github.io/server-timing/#examples

    st = {'wall_clock_time':{'dur':jd['duration'] * 1000},
          'cpu_time':{'dur':jd['cpu_time'] * 1000}}
    server_timing = ServerTiming(**st)
    response.headers["Server-Timing"] = str(server_timing)

    # wall_clock_time = 'func_cpu_time;dur={};desc="Func Call cpu time",'.format(jd['duration'])
    # cpu_time = 'func_wall_clock_timeu;dur={};desc="Func call wall clock time",'.format(jd['cpu_time'])
    # response.headers["Server-Timing"] = (cpu_time + wall_clock_time)

    # response.headers["Server-Timing"] = ('sql-1;desc="MySQL lookup Server";dur=100, '
    #                                      'sql-2;dur=900;desc="MySQL shard Server #1", '
    #                                      'fs;dur=600;desc="FileSystem", '
    #                                      'cache;dur=300;desc="Cache", '
    #                                      'other;dur=200;desc="Database Write", '
    #                                      'other;dur=110;desc="Database Read", '
    #                                      'cpu;dur=1230;desc="Total CPU"')

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
