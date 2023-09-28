import pandas as pd
import requests
from core.treintaLogger import get_logger


def scrapping_ln(logger: object):

    # import requests

    # url = "https://lndl-app.ue.r.appspot.com/out/LogInCellP2"

    # payload = "{\"Cell\":\"3203180237\",\"PassToken\":\"atlw2o\",\"FireBaseToken\":\"duiuA5ccTJqXXeDdVmuCmW:APA91bGpI_aM-m8mrwsGBZgaJW2zgJD8Iuc_3uCEV7WbCsTIDlqouOGlGIWc4fNEFkfKYgCBlMwDgE9J47azxOoNmVWD78HzeE-pBduIy-zwRfWse8xI_-_y7GgzZVRNHp5Aa8HppZcQ\",\"DeviceID\":\"68ff2769703135b1\"}"
    # headers = {
    #     "user-agent": "Dart/2.18 (dart:io)",
    #     "content-type": "application/json; charset=utf-8",
    #     "accept-encoding": "gzip",
    #     "host": "lndl-app.ue.r.appspot.com"
    # }

    # response_token = requests.post(url, data=payload, headers=headers)

    # session_id = response_token['Token'] 


    products = []

    headers = {
                "sessionId": "0a3be39adac231c74fe07cdc5aa64818aec2010aeabfd03f07507c2b",
                }

    categories = ['Whisky', 
                'Aguardiente', 
                'Cerveza', 
                'Cigarrillo', 
                'Tequila', 
                'Ron', 
                'Alimentos', 
                'Vapeadores', 
                'Vino', 
                'Espumoso', 
                'Vodka',
                'Cremas',
                'Enegizante',
                'Ginebra',
                'Bebidas',
                'Cocteleria',
                'Cognac',
                'Pal_guayabo',
                'OtrosLicores',
                'Brandy',
                'Desechables',
                'Bioseguridad'
                ]

    for j in range(len(categories)):
        i = 0
        response_products = ['']
        
        logger.debug(categories[j])
        while len(response_products) != 0:
                
                data = {
                "CatToSearch": categories[j],
                "page": i #0 - 240
                }
                
                response = requests.post(url = 'https://lndl-app.ue.r.appspot.com/pu/get_ProdsinCatsNew', headers = headers, data = data)
                print(response.json())

                response_products = response.json()['products']
                if len(response_products) != 0: 
                    products += response_products
                    logger.debug("Productos extraidos {0}, categoria {1} ".format(len(response.json()['products']), categories[j]))
                    i += 20
                else:
                    logger.debug("Cantidad total de productos: {} ".format(len(products)))
    
    prods_df =  pd.DataFrame(products)
    logger.debug("Extraidos "+str(len(prods_df))+" productos")
    products_clean = prods_df[['id',
                            'id_',
                            'nombre',
                            'presentacion_caja',
                            'presentacion',
                            'categoria_id',
                            'precio_1',
                            'promocion',
                            'tipo_promocion',
                            'Descripcion',
                            'Categoria',
                            'Sub_Categoria',
                            'Casa',
                            'alcohol',
                            'precio_T',
                            'PromoTXT',
                            'CrossPrice',
                            'CodigoBarras',
                            'CantidadCompra',
                            'iscombo'
                            ]]
    products_clean['check_date'] = str(pd.Timestamp.now(tz='America/Bogota'))

    return products_clean

