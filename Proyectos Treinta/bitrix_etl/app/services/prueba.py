from fast_bitrix24 import Bitrix
import pandas as pd
import logging

logging.getLogger('fast_bitrix24').addHandler(logging.StreamHandler())
webhook = "https://b24-hop273.bitrix24.co/rest/29/c1tkxna9k91oj120/"
b = Bitrix(webhook)
# df_leads = pd.read_excel('Prueba Leads.xlsx')
# print(df_leads.columns)
# leads = [
# 		{"fields": {
#         "TITLE": df_leads['full_name'][0],
#         "IS_RETURN_CUSTOMER": "Y",
#         "BIRTHDATE": "",
#         "SOURCE_ID": "CALL",
#         "HAS_PHONE": "Y",
#         "HAS_EMAIL": "Y",
#         "HAS_IMOL": "N",
#         "UF_CRM_1689636817918": "369",
#         "PHONE": [ { "VALUE": df_leads['phone_number'][0], "VALUE_TYPE": "WORK" }],
#   		"EMAIL": [ { "VALUE": df_leads['email'][0], "VALUE_TYPE": "WORK" } ]}},
		
# 		{"fields": {
#         "TITLE": df_leads['full_name'][1],
#         "IS_RETURN_CUSTOMER": "Y",
#         "BIRTHDATE": "",
#         "SOURCE_ID": "CALL",
#         "HAS_PHONE": "Y",
#         "HAS_EMAIL": "Y",
#         "HAS_IMOL": "N",
#         "UF_CRM_1689636817918": "369",
#         "PHONE": [ { "VALUE": df_leads['phone_number'][1], "VALUE_TYPE": "WORK" }],
#   		"EMAIL": [ { "VALUE": df_leads['email'][1], "VALUE_TYPE": "WORK" } ]}}           
#         ]

# max_concurrent_requests = 5

# with b.slow(max_concurrent_requests):
#     b.call('crm.lead.add', leads)

def extract_value(row, column_name):
    if column_name in row and isinstance(row[column_name], list):
        if len(row[column_name]) > 0:
            if 'VALUE' in row[column_name][0]:
                return row[column_name][0]['VALUE']
    return None

def get_method_list(method:str):
    data = b.get_all(method)
    print(data)
    df = pd.DataFrame(data)
    
    return df 

def get_by_id(df, method):
    consolidated_df = pd.DataFrame()
   
    list_IDS = df['ID'].to_list()
    list_IDS = [int(x) for x in list_IDS]
    data = b.get_by_ID(
        method,
        [d for d in list_IDS])

    df_new = pd.DataFrame(data)
    df_tr = df_new.transpose()

    date_columns = ['DATE_CREATE', 'DATE_REGISTER', 'DATE_MODIFY']  # Columnas de fecha que pueden variar
    users_columns = ['PHONE','EMAIL','WEB','IM']
    
    for column in date_columns:
        if column in df_tr.columns:  
            df_tr[column] = pd.to_datetime(df_tr[column], format='%Y-%m-%dT%H:%M:%S%z').dt.strftime('%Y-%m-%d %H:%M:%S')
    columns_to_drop = ['TIMESTAMP_X', 'LAST_ACTIVITY_DATE']  # Columnas a eliminar
    columns_to_drop_existing = [col for col in columns_to_drop if col in df_tr.columns]

    if columns_to_drop_existing:
        df_tr.drop(columns_to_drop_existing, axis=1, inplace=True)    

    consolidated_df = pd.concat([consolidated_df, df_tr], ignore_index=True)
    
    for column_user in users_columns:
        if column_user in consolidated_df.columns:  
            consolidated_df[column_user] = consolidated_df.apply(lambda row: extract_value(row, column_user), axis=1) 
    
    return df_tr


df = get_method_list('crm.deal.list')
final_df = get_by_id(df, 'crm.deal.get')

final_df.to_excel('prueba.xlsx', index=False)