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

from functools import wraps
from functions import validate_datetime_from_form
from collections import defaultdict

from typing import Any, NoReturn
from base64 import b64encode 
from icecream import ic
from pathlib import Path
from os import getcwd, path

import pandas as pd

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
    ic("INIT")
    db_pool = connect_to_db_from_json(path.join(getcwd(), "db.json"))
    ic("INIT")

class Test(Resource):
    def get(self):
        df = pd.read_feather("saldos.feather").set_index("index")        
        ic(df.shape)
        df = pd.read_feather("cuotas-especiales.feather").set_index("index")        
        ic(df.shape)
        return jsonify(hello="world")

class Saldos(Resource):
    def get(self):
        ic("Saldos - Get")
        ic("Reading feather")
        df = pd.read_feather("saldos.feather")
        ic("Converting into json")
        return df.to_json(date_format="iso")

    def put(self):
        """
            UPDATE FILES
        """
        ic("Saldos - put")
        conn = db_pool.get_connection()
        ic("query")
        df_old = []
        if not request.args.get("force"):
            df_old = pd.read_feather("saldos.feather").set_index("index")        
        ic(len(df_old))
        df_new = pd.read_sql_query(""" SELECT * FROM view_saldos OFFSET %s """ % len(df_old), con=conn)
        ic(df_new.shape)
        db_pool.release_connection(conn)
        if request.args.get("force"):
            df = df_new
        elif not df_new.shape[0]:
            df = df_old
        else:
            df = pd.concat([df_old, df_new])
        ic(df.shape)
        ic("converting to excel")
        df.to_excel("saldos.xlsx", sheet_name="SALDOS")  # Writting into excel
        ic("converting to feather")
        df.reset_index().to_feather("saldos.feather")  # save to feather to improve the IO time 
        ic("converting in json")
        response = df.to_json(date_format="iso")
        with open("saldos.json", "w+") as file:
            file.write(response)
        return response


class CuotasEspeciales(Resource):
    def get(self):
        ic("Saldos - Get")
        ic("Reading feather")
        df = pd.read_feather("cuotas-especiales.feather")
        ic("converting into json")
        return df.to_json(date_format="iso")

    def put(self):
        """
            UPDATE FILES
        """
        ic("cuotas")
        conn = db_pool.get_connection()
        ic("query")
        df_old = []
        if not request.args.get("force"):
            df_old = pd.read_feather("cuotas-especiales.feather").set_index("index")
        ic(len(df_old))
        df_new = pd.read_sql_query(""" SELECT * FROM view_lista_cuota_especial OFFSET %s """ % len(df_old), con=conn)
        ic(df_new.shape)
        db_pool.release_connection(conn)
        if request.args.get("force"):
            df = df_new
        elif not df_new.shape[0]:
            df = df_old
        else:
            df = pd.concat([df_old, df_new])
        ic(df.shape)
        ic("converting to excel")
        df.to_excel("cuotas-especiales.xlsx", sheet_name="CUOTAS-ESPECIALES")  # Writting into excel
        ic("converting to feather")
        df.reset_index().to_feather("cuotas-especiales.feather")  # save to feather to improve the IO time 
        ic("converting in json")
        response = df.to_json(date_format="iso")
        with open("cuotas-especiales.json", "w+") as file:
            file.write(response)
        return response


class Download(Resource):
    def get(self, file_name):
        ext = request.args.get("formato", "xlsx")
        ic(ext)
        if file_name in ("cuotas-especiales", "saldos") or ext not in ("xlsx", "json", "feather"):
            return send_file(f"{file_name}.{ext}", as_attachment=request.args.get("as_attachment", False))
        return jsonify(status="fail"), 500


api.add_resource(Test, "/catastro/")
api.add_resource(Saldos, "/catastro/datos/saldos/")
api.add_resource(CuotasEspeciales, "/catastro/datos/cuotas-especiales/")
api.add_resource(Download, "/catastro/datos/<string:file_name>/descargar/")

