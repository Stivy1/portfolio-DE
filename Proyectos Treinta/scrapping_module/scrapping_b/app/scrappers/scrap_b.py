import pandas as pd
import json
from scrapingbee import ScrapingBeeClient
import requests
from core.treintaLogger import get_logger

logger = get_logger('Scrapper_BEES', "DEBUG")

class ScrapperBEES(object):
    def __init__(self):
        self.token = self.token_bees()['access_token']
        logger.debug("Token Generado")

    def token_bees(self) -> json:
        url = "https://b2biamgbusprod.b2clogin.com/b2biamgbusprod.onmicrosoft.com/B2C_1A_SigninMobile_CO/%2FoAuth2%2Fv2.0%2Ftoken"

        payload = {
            "client_id": "4591c753-9570-415a-87f5-e74b2db8c143",
            "client_info": "1",
            "grant_type": "refresh_token",
            "refresh_token": "eyJraWQiOiJkczJMcXpQWFFVTWtmVlZZdXJKdmhzZTlkc0JiU0lJbUpDZU82R2JGWUQ0IiwidmVyIjoiMS4wIiwiemlwIjoiRGVmbGF0ZSIsInNlciI6IjEuMCJ9.W68PML7N1GaBvxktxhl0PZndTOBkxp3gYkjjjn7jM2zZbzGnFNubyRNSeNLRmrmBtd_UpnctTCEapxZXQ5Eko7fYoX33rKn8gC0xIwbRHSfOZEjhCsEqa__gyLT9FPvo2tjuOgNg9J5LWn54HAGHfDXd6XkXfAmxHwI0EIRJf2FLLici-0_vRRQr9UfxWK6EtrvweE2ZAffhfczMsBZJsPeD5cM8i_QhN2f6lijUdQaHDngGCK7TXHVghp2_R0xNCIe5lqgK9PdN2s8mznZ0-WQR_wXOZHC_vlTImI1qTPvqoOmQw5Dgql4-jbsePoIqRKS_psMNHvHEBIT3Jh45TQ.lkub7SP2YS0HThmK.niMiHdrXwOUWM8GW2U6ww8LUDeQpFRUBBZdjNLVto7eNg7-_wlZIF5ch6rHG2FFrIQsieHQNyxkpvB_O9lom-9gUAlmmHy2nPYUojWHtngt6u49KpDBrRAacnN8-WmIaLTMPQBYJWkN1HxxWR4kCV92xLSzfZpKLFjRxHeriJDLg0jYb7R3V24hPeGBkjBz7xEGUvQnwR63EBVC5DCm6GRW1RWz5K4tytzEto8_LxNVEHtBNn1bUV6ybnJ9Z5PHzbbU7tLu_G9wYwTh9scIs0lCUJtn9iOKYcKwcdhsu32tTcZ7ryrJmU0IZydiEgPa_jGUUXlWGQY5jw5eo1U-qjB5ljH6Yca-BVXhM05ZNh5Qb7b9nzdf_QM_xtGYOc-FoMgSWLlWR_T7IwXO7iefb-wkNnHHOdTb2CjBcsJLMXQTvnYgfO7VRDsUgQgaUxPD1DX4Nq0NajWHeCrxkRcRhYZu1n86JPfTeiVD5bTlQ2OTjVh1BZIXSfcJDglSnDNf50x4b97wHSL03_zobCS54jy1MJOejbIFtt6ITHt0lFtpsK9JNdFOt5xXnzPP6IAv1dKphjMYi9GY1KlakcLDSCejiFt5ew1-_qDKN28iFH6iVgBrYRhxyzg-zcc0hiBxevo3UKnha_YrTccmmG83uDDuqt7YOr6PimvmPuSX3pLnXRr1CvzZNgVfXGPS00L_8D1wwFrXDp3S4tqUhuTtCk6M4ZMYsoSe7mkSORm1ARDiH9uLLhFvH2J0N0NWF0F0ZopMU1Js-PfEIOR2pVBKbOMi0w0H4jn47kRx9o2atz471xWNjw3ceR2I3oYaHOiRFQ-ceRMgAHMY_Pfdw8n_a9miO2WYe5sA3jSjAVPWBfQfznGsmdGTm4xUduv9lrr9QV7LIgtouG70oE-LwPa96lugXBZlzRueG2lJz7XJyixmJ_i9GAVuxmjRcwDIsHKdkoL2iQXS0lpUbE6qapCPWFpMIhHCjbgDIkHw._Jt-NK8BDrxmi8i6Wpqi4g",
            "redirect_uri": "com.abi.bees.colombia://oauth/redirect",
            "scope": "openid https://b2biamgbusprod.onmicrosoft.com/4591c753-9570-415a-87f5-e74b2db8c143/user_impersonation offline_access profile",
            "x-app-name": "com.abinbev.android.tapwiser.beesColombia",
            "x-app-ver": "16.7.1"
        }
        headers = {
            "x-newrelic-id": "VQMBUFFXCxADUlZaBQIEVVM=",
            "x-app-name": "com.abinbev.android.tapwiser.beesColombia",
            "client-request-id": "4d560cc9-e7a3-4b22-b13d-f0a326afff41",
            "x-app-ver": "16.7.1",
            "x-client-cpu": "x86",
            "x-client-current-telemetry": "2|22,1|1,0,1,1,0,1",
            "x-client-last-telemetry": "2|0|||1",
            "host": "b2biamgbusprod.b2clogin.com",
            "x-client-ver": "1.5.9",
            "x-client-dm": "Android SDK built for x86",
            "x-client-os": "30",
            "x-client-sku": "MSAL.Android",
            "content-type": "application/x-www-form-urlencoded",
            "user-agent": "Dalvik/2.1.0 (Linux; U; Android 11; Android SDK built for x86 Build/RSR1.210210.001.A1)",
            "connection": "Keep-Alive",
            "accept-encoding": "gzip"
        }

        response = requests.post(url, data=payload, headers=headers)

        return response.json()

    def request_bee(self, url:str):
        trace_id = "50e3666b-611a-439f-8072-217507f206e9"
        token = "Bearer {}".format(self.token)
        country = "CO"
        regionid = "CO"
        cust_id = "13487601"
        account_id = "13487601"
        newrelic = "eyJ2IjpbMCwyXSwiZCI6eyJkLnR5IjoiTW9iaWxlIiwiZC5hYyI6IjE1NjE3NDMiLCJkLmFwIjoiMzY3ODIzMzM3IiwiZC50ciI6IjRiNDM5ZjQ2ZTM1MTQ1OTViMWU2NTQxZGYxYzE2NmNhIiwiZC5pZCI6ImE1NTMyZTc5NjlkYzQ0NzQiLCJkLnRpIjoxNjg1MzkzMzIzOTYxfX0="

        client = ScrapingBeeClient(api_key='43T9CA5AOCIO7TPCLZE8F0ZRQZYCNU5PV2AIH75R69TUJN716YNYG1PYL4EH4FPIBJLK1ZHDUKY78AMU')
        response = client.get(
            url,
            headers = {
                "Accept": "application/json",
                "accept-language": "es-CO",
                "originsystem": "ANDROID",
                "Authorization": token,
                "requesttraceid": trace_id,
                "country": country,
                "regionid": regionid,
                "custid": cust_id,
                "accountId": account_id,
                "newrelic": newrelic    
            }
        )
        return response
    
    def bees_products(self, sub_category_id:str):
        url = "https://services.bees-platform.com/v1/search/v2"

        payload = "{\"accountId\":\"13487601\",\"categoryId\":\""+sub_category_id+"\",\"page\":\"0\",\"pageSize\":\"99\",\"projections\":[\"INVENTORY\",\"PRICE\",\"SINGLE_PROMOTION\",\"CALCULATED\",\"DISCOUNTS\",\"ENFORCEMENT\",\"CATEGORIES\",\"AVAILABILITY\",\"FACETS\"],\"sortBy\":\"CATEGORY_RELEVANCE\",\"storeId\":\"4a9296fb-a28c-46e1-b71f-75a9707980be\"}"
        
        headers = {
            "country": "CO",
            "traceparent": "00-3e7dacd8dbfd4e5b8e391ed34dc89c7b-d9fe51ad2f764068-00",
            "newrelic": "eyJ2IjpbMCwyXSwiZCI6eyJkLnR5IjoiTW9iaWxlIiwiZC5hYyI6IjE1NjE3NDMiLCJkLmFwIjoiMzY3ODIzMzM3IiwiZC50ciI6IjNlN2RhY2Q4ZGJmZDRlNWI4ZTM5MWVkMzRkYzg5YzdiIiwiZC5pZCI6ImQ5ZmU1MWFkMmY3NjQwNjgiLCJkLnRpIjoxNjc1MTkzNzM2MjU2fX0=",
            "tracestate": "@nr=0-2-1561743-367823337-d9fe51ad2f764068----1675193736256",
            "authorization": "Bearer {}".format(self.token) ,
            "accept": "application/json",   
            "originsystem": "ANDROID",
            "requesttraceid": "e6908a0c-a425-459b-8c7f-c0431432118d",
            "accept-language": "es-CO",
            "content-type": "application/json; charset=UTF-8",
            "host": "services.bees-platform.com",
            "connection": "Keep-Alive",
            "accept-encoding": "gzip",
            "user-agent": "okhttp/4.9.3",
            "x-newrelic-id": "VQMBUFFXCxADUlZaBQIEVVM="
        }

        response = requests.post(url, data=payload, headers=headers)

        return response.json()
    
    def scrapping_b(self):
        brand_req = self.request_bee('https://services.bees-platform.com/api/catalog-service/v3/categories?accountId=13487601&projection=MOBILE&storeId=4a9296fb-a28c-46e1-b71f-75a9707980be')
        brand_json = brand_req.json()['categories']

        categories = []

        for i in brand_json:
            categories.append(i['categoryId'])

        #categories.remove('5fc7033c0001f40ce185c6ea')
        #categories.remove('63bdded5ccd2a43712ce8422')

        #Se extraen las subcategorias
        sub_categories = []
        for j in range(len(categories)):
            brand_req = self.request_bee('https://services.bees-platform.com/api/catalog-service/v3/categories/'+str(categories[j])+'?accountId=13487601&depth=2&page=0&pageSize=50&projection=TREE&storeId=4a9296fb-a28c-46e1-b71f-75a9707980be')
            cate_response = brand_req.json()['category']['categories']
            if cate_response:
                for k in cate_response:
                    sub_categories.append(k['categoryId'])

        products = []

        #Se extraen los productos
        for l in range(len(sub_categories)):
            brand_req = self.bees_products(sub_categories[l])
            products_json = brand_req['products']
            products += products_json
            logger.debug('Scraped {} products.'.format(str(len(products_json))))

        #Para limpiar el precio
        list_of_prices = []
        for item in products:
            price = item["price"]["price"]
            sku = item["sku"]
            list_of_prices.append({"price": price, "sku": sku})

        for item in products:
            for price_item in list_of_prices:
                if item["sku"] == price_item["sku"]:
                    item["price"] = price_item["price"]

        #Se limpian los productos, dejando solo las columans que nos interesan.
        prods_df = pd.DataFrame(products)
        products_clean = prods_df[[ 'sku',
                                    'itemName',
                                    'description',
                                    'brandName',
                                    'inventory',
                                    'price',
                                    'availability']]
        products_clean['check_date'] = str(pd.Timestamp.now(tz='America/Bogota'))
        logger.debug("Extraidos {} productos".format(str(len(products_clean))))

        return products_clean
    
    def run(self):
        try:
            result = self.scrapping_b()
            logger.debug('Scrapper Terminado con exito')
            return True, result
        except Exception as e:
            logger.debug('Scrapper Termino con errores')
            logger.debug(e)
            return False, None

if __name__ == '__main__':
    scrapper_bees = ScrapperBEES()
    scrapper_bees.run()