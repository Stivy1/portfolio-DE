import requests 
import pandas as pd
import json
from core.treintaLogger import get_logger

logger = get_logger('Scrapper_SA', "DEBUG")
def request(url):
    url = url 
    token = "Basic U3VydGlBcHA6MzFhNWE1YjAxNDBlMDEzN2UwOGFhZGFhNjBlYjAxNmE0YjQzNDgxMg=="
    headers = {
        "Accept": "application/json",
        "Authorization": token,
    }
    
    return requests.get(url, headers=headers)

def scrapper_sl():
    brand_req = request('https://intranet.surtilider.com:9001/IntranetSurti/WebServicesSurtiAppRest/consultarProductos')
    logger.debug("Inicio Scrapping")

    products = brand_req.json()['data']
    
    prods_df =  pd.DataFrame(products)
    products_clean = prods_df[['codeProduct',
                           'nameProduct',
                           'palabrasClaves',
                           'descrProduct',
                           'porcDescuento',
                           'totalValue',
                           'precioSinDcto',
                           'factorConversion',
                           'cant_nort',
                           'cant_sur'
                           ]]
    products_clean['check_date'] = str(pd.Timestamp.now(tz='America/Bogota'))
    logger.debug("Extraidos "+str(len(products_clean))+" productos")

    return products_clean