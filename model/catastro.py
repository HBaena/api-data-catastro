from .generic import Model
from psycopg2 import sql, Binary
from icecream import ic
from functions import response_to_dict
from .cripto import Cripto


class Catastro(Cripto):
    """

    """        
    def __init__(self, pool):
        super(Catastro, self).__init__(pool)
        self.pool = pool

    def get_saldos(self):
        query = """
            SELECT * FROM view_saldos
            LIMIT 10
        """

        