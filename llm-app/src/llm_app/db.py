import pathlib

from icecream import ic
# from devtools import debug


from sqlmodel import Field, Session, SQLModel, create_engine, select


class Points(SQLModel, table=True):
    id: str = Field(default=None, primary_key=True)
    point: str

# ic(uri)
# DATABASE_URL = os.environ.get("DATABASE_URL")
# engine = create_engine(DATABASE_URL, echo=True)


point1 = Points(id="", point="")


uri = "sqlite:///" + str(pathlib.Path(__file__).parent.parent.parent.absolute() / "qdrant/collection/fashion/storage.sqlite")
engine = create_engine(uri)


def select_points():
    with Session(engine) as session:
        statement = select(Points).where(
            Points.id == "gASVJAAAAAAAAACMIDk4NDY5YzA0YzEyMTRkZTNhY2U4ZWY0YTAwYjVkNDM1lC4=")
        ret = session.exec(statement).first()
        # point1.id, point1.point = f"{ret.id}_TEST", ret.point
        ic(ret.id)


def insert_points():
    point = Points(
        id="gASVJAAAAAAAAACMIDk4NDY5YzA0YzEyMTRkZTNhY2U4ZWY0YTAwYjVkND1_TEST",
        point="dummy")
    with Session(engine) as session:
        session.add(point)
        session.commit()
    ic(point)


def init_db():
    SQLModel.metadata.create_all(engine)


def get_session():
    return Session(engine)
