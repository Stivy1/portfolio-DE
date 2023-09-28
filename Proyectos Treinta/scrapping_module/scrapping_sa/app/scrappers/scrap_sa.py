import requests 
import pandas as pd
import json
from core.treintaLogger import get_logger

logger = get_logger('Scrapper_SA', "DEBUG")

LIST_IDS_DEL = ['a1c0c7e9-c274-ed11-9d78-000d3a93fe17',
    '289c9550-19b8-ec11-997e-a085fcc3b723',
    'af2bbbd0-e9ca-4a38-86b1-36fefd8a011b',
    '90383dff-e904-ea11-add2-501ac5356f6d',
    '2e7e99d2-7115-42e6-99c9-9bc95fdc9a9b',
    '5a383dff-e904-ea11-add2-501ac5356f6d',
    '8400e7d6-0d3c-404f-a906-9de38885de03',
    'bdfeda0c-55df-4636-a853-4973ca842c9c',
    'b0468b91-6449-42a0-8af4-ef22705f5b92',
    '5f383dff-e904-ea11-add2-501ac5356f6d',
    '9a411d26-ea34-eb11-9fb4-000d3a9112b0',
    '8fae6bee-12d9-eb11-a7ad-000d3a9181e7',
    '43da72e1-c912-ea11-828b-0004ffd347b6',
    '3a400c8d-a668-ea11-a94c-0004ffd34543',
    '61400c8d-a668-ea11-a94c-0004ffd34543']

LIST_NAMES_DEL = ['Tu Material POP Gratis',
    'Licores',
    'Diageo ONE',
    'Alimentos y Bebidas',
    'Dulces y Confitería',
    'Cuidado Hogar',
    'Colgate',
    "Kellogg's",
    'SC Jhonson',
    'Cuidado Personal',
    'Farmacia',
    'Papelería',
    'Mascotas',
    'Lubricantes',
    'Motos']


def request_bee_post(url, data):
    try:
        response = requests.post(
            url="https://app.scrapingbee.com/api/v1/",
            params={
                "url": url,
                "api_key": 'SH6BNJR69FBXJBH4S6EF4Z3Z9WW6X0788RIXH52STHKOTN6QZ0WTXP8IF6C5WO2LXZ7D2IE4ENVN96BM',
            },
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Accept-Encoding": "gzip, deflate, br"
            },
            data=data,
        )

        print(response.text)
    except requests.exceptions.RequestException:
        print('HTTP Request failed')
    return response.json()


def scrapping_sa():

    #Se definen variables del body para categorias
    url = "https://server.surtiapp.com.co/NeighborStore/api/v1-0/Operations"
    
    headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Accept-Encoding": "gzip, deflate, br"
            }
    
    data = {
    "$id": "1",
    "AppName": "Surtiapp",
    "ServerName": "DefaultServer",
    "Url": None,
    "SystemDefId": "403d9da0-366c-4afb-9cde-e6589743f9c4",
    "Data": {
        "$id": "2",
        "Token": {
        "$id": "3",
        "Id": "95jM9KzLCFE2dwquw0CCrApZ11x3NSiUv4kAkW5L5H7iP2GzMbBHRw=="
        },
        "Operation": {
        "$id": "4",
        "Name": "Inventory.GetClassificationCollectionOperation",
        "GroupName": None,
        "Payload": "{\"$id\":\"1\",\"Id\":\"00000000-0000-0000-0000-000000000000\",\"Name\":null,\"WithoutImage\":false,\"WithoutSortName\":false,\"ExcludeTargetedOfferFilter\":false,\"ExcludeDeletedProducts\":false,\"IncludeCategoryContent\":true,\"SortSetting\":0,\"FilterSetting\":0,\"BrandsId\":[],\"CategoriesId\":[],\"BrandName\":null,\"CategoryName\":null,\"FilterParameters\":{\"$id\":\"2\",\"ManufacturerIds\":[],\"ClassificationIds\":[],\"ProductBrandIds\":[]},\"BusinessUnitId\":\"00000000-0000-0000-0000-000000000000\",\"BranchOfficeId\":\"07d7e8fb-16ea-e911-95f9-04d3b0ad8efe\",\"CustomerAddressId\":\"55340cac-72dc-ec11-b656-a04a5e820ac5\",\"Page\":0,\"PageSize\":0,\"SerializedExpression\":null}"
        }
    }
    }


    response = requests.post(url, json.dumps(data), headers=headers)
    sd = json.loads(response.json()['data']['serializedData'])
    categories = sd['ClassificationCollection']
    list_categories = []
    categories_name = []

    for i in range(len(categories)):
        list_categories.append(categories[i]['Id'])
        categories_name.append(categories[i]['Name'])

    combined_list = list(zip(list_categories, categories_name))
    result = [(i,n) for i,n in combined_list if i not in LIST_IDS_DEL and n not in LIST_NAMES_DEL]
    list_categories, categories_name = zip(*result)

    # Se extraen los productos en base a las categorias
    products = []

    for i in range(len(list_categories)):

        response_products = ['']
        logger.debug(categories_name[i])
        
        products_body = {
        "$id": "1",
        "AppName": "Surtiapp",
        "ServerName": "DefaultServer",
        "Url": None,
        "SystemDefId": "403d9da0-366c-4afb-9cde-e6589743f9c4",
        "Data": {
            "$id": "2",
            "Token": {
            "$id": "3",
            "Id": "X987q0nF0kzZN6QIciSXweNRT0NlhogiuitZk4JmPBH3qahp++zxuQ=="
            },
            "Operation": {
            "$id": "4",
            "Name": "Inventory.LandingSearchByCategoryIdOperation",
            "GroupName": None,
            "Payload": "{\"$id\":\"1\",\"Account\":null,\"Id\":\""+list_categories[i]+"\",\"Name\":null,\"WithoutImage\":false,\"WithoutSortName\":false,\"ExcludeTargetedOfferFilter\":false,\"ExcludeDeletedProducts\":false,\"IncludeCategoryContent\":false,\"SortSetting\":0,\"FilterSetting\":0,\"BrandsId\":[],\"CategoriesId\":[],\"BrandName\":null,\"CategoryName\":null,\"FilterParameters\":{\"$id\":\"2\",\"ManufacturerIds\":[],\"ClassificationIds\":[],\"ProductBrandIds\":[]},\"BusinessUnitId\":\"00000000-0000-0000-0000-000000000000\",\"BranchOfficeId\":\"07d7e8fb-16ea-e911-95f9-04d3b0ad8efe\",\"CustomerAddressId\":\"55340cac-72dc-ec11-b656-a04a5e820ac5\",\"Page\":1,\"PageSize\":999,\"SerializedExpression\":null}"
            }
        }
        }

        response = requests.post(url, json.dumps(products_body), headers=headers)
        results = response.json()['data']['serializedData']

        response_products = json.loads(results)
        cat = response_products['Results']
        
        if len(response_products['Results']) != 0:
            for h in range(len(cat)):
                products += [cat[h]['ProductInformation']]
            logger.debug("Productos extraidos {0}, categoria {1} ".format(len(response_products['Results']), categories_name[i]))       
        else:
            logger.debug("Cantidad total de productos: {} ".format(len(products)))


    prods_df =  pd.DataFrame(products)
    products_clean = prods_df[[ 'ReferenceCode',
                                'ShortName',
                                'Price',
                                'NewPrice',
                                'DiscountPercentage',
                                'ConsumptionTax',
                                'AvailableForSale',
                                'DepartmentName',
                                'Stock',
                                'MaxQuantity',
                                'Content',
                                'ContentUnitId',
                                'ProductCategory',
                                'ProductGroup']]
    products_clean['check_date'] = str(pd.Timestamp.now(tz='America/Bogota'))
    logger.debug("Extraidos "+str(len(products_clean))+" productos")

    return products_clean