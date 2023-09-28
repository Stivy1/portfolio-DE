import pandas as pd
import requests
import json
from core.treintaLogger import get_logger

logger = get_logger('Scrapper_DAN', "DEBUG")

LIST_IDS_DEL = ['oVdR1GXY6WkAFJhgTGCQ5',
    '6jDBt4G3duyVXWBEsu5Bqy',
    '3ES1tdTag44VT91Yhd8DpL',
    '5l0PqiOZE5k1PDguTnrepG',
    '2ZO2Hlq6gMUDW0s41KjcyT',
    '4kH7qupnqOkIOboD54ydvT',
    '7k251xwxVpJa9AzI8Ffde3',
    'NvlGM8bcvREpxdIUWcXhy',
    '397WwI0evlVXBYl8PgCiba',
    '2DX8wJnCs7moeJc1dfxrEf',
    'f8WW5A7ZKOlY5W0F02wnB',
    '6eHRTtdLj57BMyO0RxEdgt',
    '5a1wvRNtCPL2dyzAFM8C2p',
    '7GOjt43W1c9tQKzz903l1O',
    '2aEZSZjCw9JERSR95LqZRG',
    '1rRHV24UkVMRavvJFeEVeo',
    '6dSoU7OSTfTiDb78ru948E',
    '2TdJsrQclxtnQuJjuxmQp7',
    '7BHMs8maEphCIZ6i6icarD',
    '6XIpfzabunqj0QOMmfJ7Pw',
    '1q9i8opG7TJxzm4ql9qVwe',
    '4eivEtOCnT9DU66myR0Vfb']

LIST_NAMES_DEL = ['B2C Categorias',
    'Bebidas',
    'Vinos',
    'Alimentos',
    'LICORES',
    'Anchetas',
    'Coctelería',
    'Vapeadores',
    'Eventos',
    'Cervezas',
    'Alimentos',
    'Accesorios',
    'Vinos',
    'Licores',
    'Bebidas',
    'Tequilas',
    'CUIDADO PERSONAL',
    'Tequilas',
    'Coctelería',
    'Accesorios',
    'CERVEZAS']

class ScrapperDAN(object):
    def __init__(self):
        self.token = self.token_dan()['data']['idToken']
        logger.debug("Token Generado")
        logger.debug(self.token)

    def token_dan(self) -> json:
        url = "https://2cp03dy8ye.execute-api.us-east-1.amazonaws.com/pdn/b2b/authentication/api/v1/login"
        payload = {
            "username": "1018505654",
            "password": "1123",
            "captcha": "b45f2df4986ef953"
        }
        headers = {
            "accept": "application/json, text/plain, */*",
            "ip-address": "192.168.0.22",
            "content-type": "application/json",
            "host": "2cp03dy8ye.execute-api.us-east-1.amazonaws.com",
            "connection": "Keep-Alive",
            "accept-encoding": "gzip",
            "user-agent": "okhttp/3.12.12"
        }

        response = requests.post(url, json=payload, headers=headers)
        return response.json()
    
    def clean_categories(self):
        url = "https://search-orn-orn-else-dom-orion-pdn-cieaqpo2euqmob25r7wkma4yzq.us-east-1.es.amazonaws.com/b2b/_search/"

        payload = {
            "query": { "match": { "contentType": "categoria" } },
            "size": 10000
        }
        headers = {
            "accept": "application/json, text/plain, */*",
            "authorization": "Basic b3JuLWVsc2UtdXNyLXBkbjpPcmlvbkAyMlByb2plY3Q=",
            "content-type": "application/json",
            "host": "search-orn-orn-else-dom-orion-pdn-cieaqpo2euqmob25r7wkma4yzq.us-east-1.es.amazonaws.com",
            "connection": "Keep-Alive",
            "accept-encoding": "gzip",
            "user-agent": "okhttp/3.12.12"
        }

        response = requests.post(url, json=payload, headers=headers)

        name_category = []
        id_category = []

        category = response.json()['hits']['hits']
        for i in range(len(category)):
            name_category.append(category[i]['_source']['fields']['nombre']['es-CO'])
            id_category.append(category[i]['_id'])

        combined_list = list(zip(id_category, name_category))
        result = [(i,n) for i,n in combined_list if i not in LIST_IDS_DEL and n not in LIST_NAMES_DEL]
        id_category, name_category = zip(*result)
        logger.debug("Lista categorias {0}".format(id_category))
        return id_category, name_category
#
    def dan_products(self, category:str ) -> json:
        """
            Funtion that retunr the json with products of a specific category

            Parameters
            ----------
            token : str
                Token for DAN Scrapper name
            category : str
                Id of Category
            Return
        """
        url = "https://2cp03dy8ye.execute-api.us-east-1.amazonaws.com/pdn/b2b/product/api/v1/product"

        querystring = {
            "subsidiary": "001",
            "page": "0",
            "size": "999",
            "category": category,
            "tradeAgreement": "false"
        }

        headers = {
            "accept": "application/json, text/plain, */*",
            "ip-address": "192.168.0.22",
            "authorization": self.token,
            "host": "2cp03dy8ye.execute-api.us-east-1.amazonaws.com",
            "connection": "Keep-Alive",
            "accept-encoding": "gzip",
            "user-agent": "okhttp/3.12.12",
            "if-modified-since": "Mon, 09 Feb 2023 15:16:52 GMT"
        }

        response = requests.get(url, headers=headers, params=querystring)
        return response.json()

    

    def scrapping_dan(self):

        products = []
        id_category, name_category = self.clean_categories()

        for i in range(len(id_category)):
            prod_json = self.dan_products(id_category[i])
            logger.debug("Product category: {0}".format(id_category[i]))
            #logger.debug(prod_json)
            products_json = prod_json['data']
            products += products_json
            
            logger.debug("Productos extraidos {0}, categoria {1} ".format(len(products_json), name_category[i]))
        prods_df = pd.DataFrame(products)
        products_clean = prods_df[[
                            'sku',
                            'name',
                            'price',
                            'priceWithDiscount',
                            'priceNet',
                            'discount',
                            'content',
                            'stock',
                            'units']]

        products_clean['check_date'] = str(pd.Timestamp.now(tz='America/Bogota'))

        return products_clean

    def run(self):
        try:
            result = self.scrapping_dan()
            logger.debug(result)
            logger.debug('Scrapper Terminado con exito')
            return True, result
        except Exception as e:
            logger.debug('Scrapper Termino con errores')
            logger.debug(e)
            return False, None
        
if __name__ == '__main__':
    scrapper_dan = ScrapperDAN()
    scrapper_dan.run()