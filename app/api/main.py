from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session
from .db import SessionLocal, engine, Base
from sqlalchemy import Table, MetaData

Base.metadata.create_all(bind=engine)

metadata = MetaData()
metadata.reflect(bind=engine, schema="public")
table = Table("gold_final_accidents", metadata, autoload_with=engine, schema="public")
app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.get("/table/", tags=[f"datamart : {table}"])
def table_contents(page: int = 0, limit: int = 1, db: Session = Depends(get_db)):
    results = db.query(table).offset(page).limit(limit).all()
    return [dict(row._mapping) for row in results]

