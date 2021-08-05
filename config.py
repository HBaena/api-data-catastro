from flask import Flask
from flask_restful import Api  # modules for fast creation of apis
from os import path, getcwd
from DB import PosgresPoolConnection
from psycopg2 import pool
from datetime import timedelta

def connect_to_db_from_json(filename: str) -> pool:
    return PosgresPoolConnection(path.join(getcwd(), filename))

app = Flask(__name__)  # Creating flask app
app.secret_key = "gydasjhfuisuqtyy234897dshfbhsdfg83wt7"
api = Api(app)  # Creating API object from flask app

