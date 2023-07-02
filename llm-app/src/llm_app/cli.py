#!/usr/bin/env python3

import click
from dotenv import load_dotenv

from db import get_session, Points, insert_points
from llm import get_embeddings
from sqlmodel import select

from icecream import ic
# from devtools import debug

load_dotenv()


@click.group()
def cli_db():
    pass


@cli_db.command()
def db_insert():
    """Command on db_insert"""
    insert_points()


@cli_db.command()
def db_exec():
    """Command on db_exec"""
    # select_points()
    session = get_session()
    statement = select(Points).where(
        Points.id == "gASVJAAAAAAAAACMIDk4NDY5YzA0YzEyMTRkZTNhY2U4ZWY0YTAwYjVkNDM1lC4=")
    ret = session.execute(statement).first()
    # point1.id, point1.point = f"{ret.id}_TEST", ret.point
    ic(ret)


@click.group()
def cli_llm():
    pass


@cli_llm.command()
def llm_exec():
    """Command on llm_exec"""
    get_embeddings()


cli = click.CommandCollection(sources=[cli_db, cli_llm])

if __name__ == '__main__':
    cli()
