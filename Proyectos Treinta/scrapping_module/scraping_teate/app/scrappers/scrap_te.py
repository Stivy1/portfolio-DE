import pandas as pd
import requests
from core.treintaLogger import get_logger


def scrapping_te(logger: object):

    url = "https://mobile-h2ca5f576.us3.hana.ondemand.com/oData_CLNTHB8300/ZTAT_04_DEF_SRV/CanastasSet"

    querystring = {
        "$format": "json",
        "$filter": "Prdha eq ''"
    }

    headers = {
        "authorization": "Bearer 7da4a4d05a22f507642ed672a9836cc",
        "accept": "application/json",
        "x-smp-appid": "com.teatedigital.teateapp",
        "content-type": "application/json",
        "x-smp-deviceid": "68ff2769703135b1",
        "x-smp-sdk-version": "SAPCPSDKFORAND-2.2.0",
        "host": "mobile-h2ca5f576.us3.hana.ondemand.com",
        "connection": "Keep-Alive",
        "accept-encoding": "gzip",
        "user-agent": "okhttp/4.9.0"
    }

    response_cat = requests.get(url, headers=headers, params=querystring)
    categorias = response_cat.json()['d']['results']

    for d in categorias:
        d.pop('__metadata', None)
        d.pop('Phlink', None)

    url = "https://mobile-h2ca5f576.us3.hana.ondemand.com/oData_CLNTHB8300/ZTAT_11_DEF_SRV/PedidosSet"

    querystring = {
        "$format": "json",
        "$filter": "Kunnr eq '20030917' and Wotnr eq '2'"
    }


    response = requests.get(url, headers=headers, params=querystring)

    products = response.json()['d']['results']

    for d in products:
        d.pop('__metadata', None)
        d.pop('Msgid', None)
        d.pop('Msgtx', None)
        d.pop('Fileextern', None)
        d.pop('Kunnr', None)
        d.pop('Maximo', None)
    
    prods_df = pd.DataFrame(products)
    cat_df = pd.DataFrame(categorias)

    products_total = pd.merge(prods_df, cat_df, how='left', on='Prdha')
    logger.debug("Productos extraidos {}".format(len(products_total)))
    columns_price = ['Zpre', 'Zsug', 'Zfia']
    for col in columns_price:
        products_total[col] = products_total[col].replace('\.', '', regex=True).astype(int)

    
    products_clean = products_total[[
            'Matnr',
            'Maktx',
            'Meinh',
            'Vtext',
            'Zpre',
            'Zpro',
            'Zsug',
            'Datum'
        
    ]]
    products_clean = products_clean.rename(columns={'Matnr': 'sku',
                                    'Maktx':'nombre_producto',
                                    'Meinh':'unidad',
                                    'Vtext': 'categoria',
                                    'Zpre': 'precio',
                                    'Zpro': 'precio_descuento',
                                    'Zsug': 'precio_sugerido'})
    products_clean['check_date'] = str(pd.Timestamp.now(tz='America/Bogota'))

    return products_clean

