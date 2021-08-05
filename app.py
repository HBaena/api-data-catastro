from flask import request  # Flask, receiving data from requests, json handling
from flask import send_file  # Create url from statics files
from flask import send_from_directory  # Create url from statics files
from flask import make_response
from flask import jsonify  # Flask, receiving data from requests, json handling
from flask import after_this_request  
from flask_restful import Resource  # modules for fast creation of apis

from config import app
from config import api
from config import connect_to_db_from_json

from model import catastro

from functools import wraps
from functions import validate_datetime_from_form
from collections import defaultdict

from typing import Any, NoReturn
from base64 import b64encode 
from icecream import ic
from pathlib import Path

db_pool = None
@app.after_request
def after_request(response) -> Any:
    """
    Prevent CORS problems after each `request
    :param response: Response of any request
    :return: The same request
    """
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE, PATCH')
    return response

@app.before_first_request
def initialize() -> NoReturn:
    """
    Function: initialize
    Summary: Functions that start services that the appi needs
    Examples: validate licenses, create db connections or start face recognition service 
    Returns: Nothing
    """
    global db_pool, catastro_model
    pool = connect_to_db_from_json(path.join(getcwd(), "db.json"))
    ic("INIT")

class Test(Resource):
    def get(self):
        return jsonify(hello="world")

class Saldos(Resource):
    def get(self)
        import pandas as pd
        conn = pool.get_connection()
        pd.read_sql_query("""
            SELECT * FROM view_saldos
                    LIMIT 10
                    """, conn=conn)
        return jsonify()
        pool.release_connection(conn)


api.add_resource(Test, "/catastro/")
api.add_resource(Saldos, "/catastro/datos/saldos/")
