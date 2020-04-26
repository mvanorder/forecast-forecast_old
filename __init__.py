import pymongo
from pymongo import MongoClient
from config import host, port, uri
from ETL.db_ops import Client

client = Client(host, port)
remote_client = Client(uri)