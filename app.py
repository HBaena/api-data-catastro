from flask import request  # Flask, receiving data from requests, json handling
from flask import send_file  # Create url from statics files
from flask import send_from_directory  # Create url from statics files
from flask import make_response
from flask import jsonify  # Flask, receiving data from requests, json handling
from flask import after_this_request  
from flask_restful import Resource  # modules for fast creation of apis

from werkzeug.exceptions import HTTPException
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
from time import time
from datetime import datetime


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
        df_1 = pd.read_feather("saldos.feather").set_index("index")        
        ic(df_1.shape)
        df_2 = pd.read_feather("cuotas-especiales.feather").set_index("index")        
        ic(df_2.shape)
        return jsonify(hello="world", saldos=df_1.shape, cuotas_especiales=df_2.shape)

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
        t_0 = time()
        df_new = pd.read_sql_query(""" SELECT * FROM saldos OFFSET %s """ % len(df_old), con=conn)
        t_read = time() - t_0
        ic(df_new.shape)
        db_pool.release_connection(conn)
        if request.args.get("force"):
            df = df_new
        elif not df_new.shape[0]:
            df = df_old
        else:
            df = pd.concat([df_old, df_new])
        # ic("converting to excel")
        # df.to_excel("saldos.xlsx", sheet_name="SALDOS")  # Writting into excel
        ic("converting to feather")
        t_0 = time()
        df.reset_index().to_feather("saldos.feather")  # save to feather to improve the IO time 
        df = df.sort_values(["fecha asignacion", "fecha pago"], ascending=False)
        df.to_parquet("saldos.parquet")  # save to feather to improve the IO time 
        t_write = time() - t_0
        # ic("converting in json")
        # response = df.to_json(date_format="iso")
        # with open("saldos.json", "w+") as file:
            # file.write(response)
        return jsonify(updated=True, time_read=t_read, time_write=t_write)


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
        t_0 = time()
        df_new = pd.read_sql_query(str(open("query.sql", "r").read() % len(df_old)), con=conn)
        t_read = time() - t_0
        ic(df_new.shape)
        db_pool.release_connection(conn)
        if request.args.get("force"):
            df = df_new
        elif not df_new.shape[0]:
            df = df_old
        else:
            df = pd.concat([df_old, df_new])
        # ic("converting to excel")
        # df.to_excel("cuotas-especiales.xlsx", sheet_name="CUOTAS-ESPECIALES")  # Writting into excel
        ic(df.shape)

        cols = ['id_usuario', 'cuenta', 'movimiento', 'nombre', 'comentarios', 'efecto',        
                               'situacion', 'aprobado', 'aplicado', 'fecha asignacion', 'padron',
                               'ubicacion', 'valor fiscal', 'observaciones', 'usuario', 'periodo inicio', 
                               'periodo fin', 'monto a pagar',                 
                               'recibo', 'folio', 'caja', 'fecha pago']
        df.columns = cols  # Rename cols for better understanding
        cols = [
            'cuenta', 'nombre', 'comentarios', 'efecto', 'movimiento', 'situacion', 'aprobado', 'aplicado', 
            'valor fiscal', 'observaciones', 'usuario', 'monto a pagar', 'recibo', 'folio', 'caja', 
            'ubicacion', 'fecha pago', 'fecha asignacion']
        ic()
        df["monto a pagar"].fillna(0.0, inplace=True)
        # df["monto a pagar"] = np.ceil(df["monto a pagar"]) 
        df.efecto = df.efecto.astype(int)
        df["fecha pago"] = pd.to_datetime(df["fecha pago"]).dt.strftime("%Y-%m-%d %H:%M")
        df["fecha asignacion"] = pd.to_datetime(df["fecha asignacion"]).dt.strftime("%Y-%m-%d %H:%M")
        df["caja"].fillna("NaN", inplace=True)
        df["folio"].fillna("NaN", inplace=True)
        df["movimiento"].replace({
            "SIS": "Sistema de interés social",
            "STE": "Sistema de 3ra edad",
            "SJP": "Sistema de jubilado pensionado",
            "DAS": "Sistema de asistencia social",
            "SDC": "Sistema de discapacidad",
            "SDI": "Sistema de discapacidad",
        }, inplace=True)
        ic("converting to feather")
        t_0 = time()
        df.reset_index().to_feather("cuotas-especiales.feather")  # save to feather to improve the IO time 
        df = df.sort_values(["fecha asignacion", "fecha pago"], ascending=False)
        print("to parquet ---------------------------------------------")
        df.to_parquet("cuotas-especiales.parquet")  # save to feather to improve the IO time 
        t_write = time() - t_0
        # ic("converting in json")
        # response = df.to_json(date_format="iso")
        # with open("cuotas-especiales.json", "w+") as file:
            # file.write(response)
        return jsonify(updated=True, time_read=t_read, time_write=t_write)


class PagoRecibo(Resource):
    def get(self):
        ic("Saldos - Get")
        ic("Reading feather")
        df = pd.read_feather("pagos-recibo.feather")
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
            df_old = pd.read_feather("pagos-recibo.feather").set_index("index")
        ic(len(df_old))
        t_0 = time()
        df_new = pd.read_sql_query(""" SELECT view_pago_recibo """ % len(df_old), con=conn)
        t_read = time() - t_0
        ic(df_new.shape)
        db_pool.release_connection(conn)
        if request.args.get("force"):
            df = df_new
        elif not df_new.shape[0]:
            df = df_old
        else:
            df = pd.concat([df_old, df_new])
        # ic("converting to excel")
        # df.to_excel("pagos-recibo.xlsx", sheet_name="pagos-recibo")  # Writting into excel
        ic("converting to feather")
        t_0 = time()
        df.reset_index().to_feather("pagos-recibo.feather")  # save to feather to improve the IO time 
        df = df.sort_values(["fecha asignacion", "fecha pago"], ascending=False)
        df.to_parquet("pagos-recibo.parquet")  # save to feather to improve the IO time 
        t_write = time() - t_0
        # ic("converting in json")
        # response = df.to_json(date_format="iso")
        # with open("cuotas-especiales.json", "w+") as file:
            # file.write(response)
        return jsonify(updated=True, time_read=t_read, time_write=t_write)


class Pagos(Resource):
    def get(self):
        ic("Cartera - Get")
        ic("Reading feather")
        df = pd.read_feather("pagos.feather")
        ic("converting into json")
        return df.to_json(date_format="iso")

    def put(self):
        """
            UPDATE FILES
        """
        ic("Cartera")
        conn = db_pool.get_connection()
        ic("query")
        t_0 = time()
        df = pd.read_sql_query(""" SELECT * FROM view_cartera """, con=conn)
        t_read = time() - t_0
        db_pool.release_connection(conn)
        ic("converting to feather")
        t_0 = time()
        df.reset_index().to_feather("pagos.feather")  # save to feather to improve the IO time 
        # df = df.sort_values(["fecha asignacion", "fecha pago"], ascending=False)
        df.to_parquet("pagos.parquet")  # save to feather to improve the IO time 
        t_write = time() - t_0
        return jsonify(updated=True, time_read=t_read, time_write=t_write)


class CarteraCatastral(Resource):
    def get(self):
        ic("Cartera - Get")
        ic("Reading feather")
        df = pd.read_feather("cartera-catastral.feather")
        ic("converting into json")
        return df.to_json(date_format="iso")

    def put(self):
        """
            UPDATE FILES
        """
        ic("Cartera")
        conn = db_pool.get_connection()
        ic("query")
        t_0 = time()
        df = pd.read_sql_query(""" SELECT * FROM view_lista_pagos """, con=conn)
        t_read = time() - t_0
        db_pool.release_connection(conn)
        ic("converting to feather")
        t_0 = time()
        df.reset_index().to_feather("cartera-catastral.feather")  # save to feather to improve the IO time 
        # df = df.sort_values(["fecha asignacion", "fecha pago"], ascending=False)
        df.to_parquet("cartera-catastral.parquet")  # save to feather to improve the IO time 
        t_write = time() - t_0
        return jsonify(updated=True, time_read=t_read, time_write=t_write)


class Download(Resource):
    def get(self, file_name):
        ext = request.args.get("formato", "xlsx")
        ic(ext)
        if file_name in ("cuotas-especiales", "saldos", "pagos", "cartera-catastral") or ext not in ("xlsx", "json", "feather", "parquet"):
            return send_file(f"{file_name}.{ext}", as_attachment=request.args.get("as_attachment", False))
        return jsonify(status="fail")


class FileInfo(Resource):
    def get(self, file_name):
            # file = "http://miros.geovirtual.mx:5005/catastro/datos/cuotas-especiales/descargar/?formato=feather"
            # response = requests.get(url, stream=True)
            # dir(response.raw)
            # import os.path, time
            try:
                fname = Path(f'{file_name}.feather')
                return jsonify(modified= datetime.fromtimestamp(fname.stat().st_mtime).strftime('%Y:%m:%d %H:%M'))
            except Exception as e:
                return jsonify(modified=None, log=str(e)), 501


api.add_resource(Test, "/catastro/")
api.add_resource(Saldos, "/catastro/datos/saldos/")
api.add_resource(CuotasEspeciales, "/catastro/datos/cuotas-especiales/")
api.add_resource(PagoRecibo, "/catastro/datos/pago-recibo/")
api.add_resource(CarteraCatastral, "/catastro/datos/cartera-catastral/")
api.add_resource(Pagos, "/catastro/datos/pagos/")
api.add_resource(Download, "/catastro/datos/<string:file_name>/descargar/")
api.add_resource(FileInfo, "/catastro/datos/<string:file_name>/info/")

@app.errorhandler(Exception)
def handle_exception(e):
        if db_pool.pool.closed:
            initialize()
            return jsonify(msg="db error", status_code=502)
        else: 
            return jsonify(msg="error", status_code=502, log=str(e))
