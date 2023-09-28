from fast_bitrix24 import Bitrix
import logging
import pandas as pd
from services.leads_extract import extract_leads
from services.bigquery import dataframe_to_BQ
from core.treintaLogger import get_logger

logging.getLogger('fast_bitrix24').addHandler(logging.StreamHandler())

logger = get_logger('Leads FB to Bitrix', "DEBUG")

max_concurrent_requests = 5

webhook = "https://b24-hop273.bitrix24.co/rest/29/c1tkxna9k91oj120/"
b = Bitrix(webhook)

df_leads = extract_leads()

if df_leads.empty is True:
    logger.debug("No hay Leads Nuevos por enviar")
    exit()
    
for i in range(len(df_leads)):    
    deals_data = {
                "fields": { 
                "TITLE": df_leads['TITLE'][i],
                "NAME": df_leads['NAME'][i],
                "LAST_NAME": df_leads['LAST_NAME'][i],
                "HAS_PHONE": df_leads['HAS_PHONE'][i],
                "HAS_EMAIL": df_leads['HAS_EMAIL'][i],
                "ASSIGNED_BY_ID": df_leads['ASSIGNED_BY_ID'][i],
                "PHONE": [{ "VALUE": df_leads['PHONE'][i], "VALUE_TYPE": "WORK" }],
                "EMAIL": [{ "VALUE": df_leads['EMAIL'][i], "VALUE_TYPE": "WORK" }],
                "UF_CRM_1689636817918": df_leads['UF_CRM_1689636817918'][i],
                "UF_CRM_1689636855596": df_leads['UF_CRM_1689636855596'][i],
                "UF_CRM_1689637120616": df_leads['UF_CRM_1689637120616'][i],
                "UF_CRM_1689637086280": df_leads['UF_CRM_1689637086280'][i],
                "UF_CRM_1691424533787": df_leads['UF_CRM_1691424533787'][i],
                "UF_CRM_1692976516": df_leads['UF_CRM_1692976516'][i],
                "UF_CRM_1689636984854": df_leads['UF_CRM_1689636984854'][i],
                }}
    print(deals_data)    
    with b.slow(max_concurrent_requests):
        try:
            b.call('crm.lead.add', deals_data)
        except:
            pass
response_BQ = dataframe_to_BQ(df_leads, "data-production-337318.bitrix_leads.facebook_leads_uploaded")
logger.debug(F"{response_BQ}")