import pandas as pd
import numpy as np
from scrapingbee import ScrapingBeeClient
import requests


def scrapping_ca(logger: object):

    url = 'https://corbeapp-api.movilventas.com/marks-and-categories'
    payload = { "channel": "TIENDAS" }

    brand_req = requests.post(url=url, data=payload)
    brand_json = brand_req.json()['categories']
    print(brand_json)
    categories = [cat['name'] for cat in brand_json]

    #categories.remove('INGRED Y MEZCLAS PARA PREPARAR')
    #categories.remove('XRANCHO Y NO BASICOS')
    #categories.remove('LECHES')
    #categories.remove('ANCHETAS')

    products = []

    for i in range(len(categories)):
        try:
            logger.debug('Category: {}'.format(str(categories[i])))
            products_url = 'https://corbeapp-api.movilventas.com/products'
            data = {
            "customer": "9014096574",
            "limit": 250,
            "page": 1,
            "channel": "TIENDAS",
            "search_criteria": "",
            "zone": "APP001",
            "businessUnit": "DIBOG",
            "catalog": "CORBETAWEB",
            "promotion": "",
            "productType": "GENERIC",
            "brand": "",
            "subline": str(categories[i]),
            "minQTY": 5
                }
            products_req = requests.post(products_url, data)
            products_json = products_req.json()['data']['products']
        except:
            products_json = []
            pass
        logger.debug('Scraped {} products.'.format(str(len(products_json))))
        products.append(products_json)


    arr_prods = np.concatenate(products)
    prods_df = pd.DataFrame(list(arr_prods))
    products_clean = prods_df[['ean',
                            'priceList',
                            'discountPrice',
                            'netPrice',
                            'unitOfMeasure',
                            'nameByMagento',
                            'subline']]
    products_clean['check_date'] = str(pd.Timestamp.now(tz='America/Bogota'))
    products_clean['priceList'] = products_clean['priceList'].astype(int)

    return products_clean
