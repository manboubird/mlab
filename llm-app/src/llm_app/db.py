#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os

from icecream import ic
from devtools import debug

from typing import Optional

from sqlmodel import Field, Session, SQLModel, create_engine, select


class Points(SQLModel, table=True):
    id: str = Field(default=None, primary_key=True)
    point: str

engine = create_engine("sqlite:///" + os.path.abspath("./qdrant/collection/fashion/storage.sqlite"))

point1 = Points(id="", point="")

with Session(engine) as session:
    statement = select(Points).where(Points.id == "gASVJAAAAAAAAACMIDkyNDhkMWFjNDE2MzQ1ZjhiY2Q2MmYyMGUwNmMwOWM3lC4=_TEST")
    ret = session.exec(statement).first()
    # point1.id, point1.point = f"{ret.id}_TEST", ret.point
    ic(ret.id)

# ic(point1.id)


# row insertion
# SQLModel.metadata.create_all(engine)
# with Session(engine) as session:
#     session.add(point1)
#     session.commit()
