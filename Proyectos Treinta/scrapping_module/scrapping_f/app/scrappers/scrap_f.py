import pandas as pd
from scrapingbee import ScrapingBeeClient
import numpy as np
from core.treintaLogger import get_logger

logger = get_logger('Scrapper_BEES', "DEBUG")

def request_bee(url:str):
    client = ScrapingBeeClient(api_key='43T9CA5AOCIO7TPCLZE8F0ZRQZYCNU5PV2AIH75R69TUJN716YNYG1PYL4EH4FPIBJLK1ZHDUKY78AMU')
    response = client.get(
        url,
        headers = {
        "Accept": "application/json"
        }
    )
    return response

def scrapping_f(logger:object):
    brand_req = request_bee('https://bog.frubana.com/api/v1/catalog_v2/categories/active')
    brand_json = brand_req.json()

    categories = []

    for i in brand_json:
        categories.append(i['id'])
        categories

    products = []

    for i in range(len(categories)):
        brand_req = request_bee('https://bog.frubana.com/api/v1/catalog_v2/search/category/'+str(categories[i])+'?pageSize=1000&page=0&q=*:*')
        products_json = brand_req.json()['products']
        
        products.append(products_json)
        logger.debug('Scraped {} products.'.format(str(len(products_json))))

    arr_prods = np.concatenate(products)
    prods_df = pd.DataFrame(list(arr_prods))
    logger.debug("Extraidos {} productos".format(str(len(prods_df))))
    products_clean = prods_df[['id',
                            'name',
                            'price',
                            'salePrice',
                            'discountPercentage',
                            'category']]
    products_clean['check_date'] = str(pd.Timestamp.now(tz='America/Bogota'))
    return products_clean