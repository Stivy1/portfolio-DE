import pandas as pd
from scrapingbee import ScrapingBeeClient
from core.treintaLogger import get_logger
import numpy as np

logger = get_logger('Scrapper_BEES', "DEBUG")

def request_bee(url:str):
    client = ScrapingBeeClient(api_key='43T9CA5AOCIO7TPCLZE8F0ZRQZYCNU5PV2AIH75R69TUJN716YNYG1PYL4EH4FPIBJLK1ZHDUKY78AMU')
    response = client.get(
        url,
        headers = {
            "Accept": "application/json",
            "accept-language": "es-CO",
            "accept-encoding": "gzip",
            "app-version": "11.5.7",
            "authorization": "IJASDFIJHADFJASKHJFASKLJHFAJLSHKF",
        }
    )
    return response
    
def scrapping_c(logger: object):
    brand_req = request_bee(url = 'https://search-bar.chiper.co/store/354713/searches?warehouseIds=[184]&platform=app&isElastic=true&macrocategory=-1&lng=es-CO&appVersion=11.5.4')
    brand_json = brand_req.json()['macrocategoriesUser']
    categories = []
    categories_name = []
    for i in range(len(brand_json)):
        categories.append(str(brand_json[i]['id']))
        categories_name.append(str(brand_json[i]['name']))

    categories.remove('-1')
    categories_name.remove('Todos')

    products_list = []
    for i in range(len(categories)):
        sub_categories = []
        sub_categories_name = []
        sub_cat_req = request_bee('https://catalogue.chiper.co/store/354713/macros/{}/subcategories?locationId=2&warehouseIds=%5B184%5D&ecommerce=true&lng=es-CO&limit=20&appVersion=11.5.7&hideCigarettes=true'.format(categories[i]))
        sub_cat_json = sub_cat_req.json()
        for j in range(len(sub_cat_json)):
            sub_categories.append(sub_cat_json[j]['subCategoryId'])
            sub_categories_name.append(sub_cat_json[j]['subCategoryName'])
        for k in range(len(sub_categories)):
            try:
                products_req = request_bee('https://catalogue.chiper.co/store/354713/subcategories/{}?&skip=0&limit=999&lng=es-CO&appVersion=11.5.7&hideCigarettes=true'.format(sub_categories[k]))
                print("Scraped {0} products from {1}".format(len(products_req.json()['avaibleStock']), sub_categories_name[k]))
                print(sub_categories_name[k])
                products_list.append(products_req.json()['avaibleStock'])
            except:
                pass
            
    arr_prods = np.concatenate(products_list)
    prods_df = pd.DataFrame(list(arr_prods))
    products_clean = prods_df[[
            'id', 'sku','name','packagingType',
            'macroId', 'subCategoryId', 'brandId',
            'companyId', 'referenceId', 
            'createdAt','stock','minQuantity', 'warehouseId', 'locationId',
            'managerMaximumDelivery', 'maximumQuantity', 'scaleType',
            'customerPrice', 'saves', 'recomendedPVP', 'discountedMaximumQuantity',
            'discount', 'prices']]
    products_clean['id'] = products_clean['id'].astype(str)
    products_clean['companyId'] = products_clean['companyId'].astype(str)
    products_clean['macroId'] = products_clean['macroId'].astype(str)
    products_clean['check_date'] = str(pd.Timestamp.now(tz='America/Bogota'))
    logger.debug("check_date {}".format(products_clean['check_date'][0]))
    
    return products_clean