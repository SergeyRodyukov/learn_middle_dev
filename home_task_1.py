from typing import List
import databases
import sqlalchemy
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime,date, timedelta


#описание параметров подключения к БД
DB_USER = "postgres"
DB_NAME = "StoreDataBase"
DB_PASSWORD = "gkrqnhjd"
DB_HOST = "127.0.0.1"

#Подключение к БД. Создание пула соеденений и сессии
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5435/{DB_NAME}"

metadata = sqlalchemy.MetaData()
database = databases.Database(DATABASE_URL)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

#Создание таблиц с помощью наследования от класса Table
store_table = sqlalchemy.Table(
    "store",
    metadata,
    sqlalchemy.Column("identifier", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("address", sqlalchemy.String),

)

item_table = sqlalchemy.Table(
    "item",
    metadata,
    sqlalchemy.Column("identifier", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String, unique=True),
    sqlalchemy.Column("price", sqlalchemy.Float),
)

sales_table = sqlalchemy.Table(
    "sales",
    metadata,
    sqlalchemy.Column("identifier", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("sale_time", sqlalchemy.DateTime, default = datetime.now()),
    sqlalchemy.Column("item_id", sqlalchemy.Integer, sqlalchemy.ForeignKey ("item.identifier")),
    sqlalchemy.Column("store_id", sqlalchemy.Integer, sqlalchemy.ForeignKey ("store.identifier")),
)

#Создание таблиц, если их нет
metadata.create_all(engine)

now = datetime.now()

#Классы описывающие представление вывода запросов
class StoreList(BaseModel):
    identifier:int
    address: str


class ItemList(BaseModel):
    identifier:int
    name: str
    price: float

class SalesList(BaseModel):
    item_id: int
    store_id: int
    sale_time= now

class StoreTop(BaseModel):
    identifier: int
    address: str
    summ_sales: float

class ItemTop(BaseModel):
    identifier: int
    name: str
    count_item: int

app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

#get запрос на вывод списка всех магазинов
@app.get("/listStores/", response_model=List[StoreList])
async def read_listStore():
    query = store_table.select()
    return await database.fetch_all(query)

#get запрос на вывод списка всех товаров
@app.get("/listItem/", response_model=List[ItemList])
async def read_listItem():
    query = item_table.select()
    return await database.fetch_all(query)

#post запрос а добавление продажи
@app.post("/listSales/", response_model=SalesList)
async def create_sale(note : SalesList):
    query = sales_table.insert().values(item_id=note.item_id, store_id=note.store_id, sale_time=note.sale_time)
    last_record_id = await database.execute(query)
    return {**note.dict(), "id": last_record_id}

#get запрос на топ 10 самых продаваемых товаров
@app.get("/top/item/", response_model=List[ItemTop])
async def top_item():
    query = f"""
    SELECT item.identifier, item.name, COUNT(item.name) as count_item
    FROM sales
	JOIN store on sales.store_id = store.identifier
	JOIN item on sales.item_id = item.identifier
	GROUP BY item.identifier
	ORDER BY count_item DESC
	LIMIT 10
    """
    return await database.fetch_all(query)


#get запрос на топ 10 самых доходных магазинов
@app.get("/top/store/", response_model=List[StoreTop])
async def top_store():
    date_sale = date.today() - timedelta(days=30)
    query = f"""
    select store.identifier, store.address, sum(item.price) as summ_sales
    from sales 
	join store on sales.store_id = store.identifier
	join item on sales.item_id = item.identifier
	where sales.sale_time >= '{date_sale}'
	group by store.identifier
	order by summ_sales desc
	limit 10
    """
    return await database.fetch_all(query)

  
