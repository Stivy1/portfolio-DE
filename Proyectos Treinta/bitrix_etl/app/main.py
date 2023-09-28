import pandas as pd
from services.bigquery import dataframe_to_BQ
from services.bigquery import append_to_BQ
from services.bigquery import update_to_BQ
from services.bigquery import delete_to_BQ
from services.bigquery import append_historic_to_BQ
from services.bitrix_endpoints import get_method_list
from services.bitrix_endpoints import get_by_id
from services.bitrix_endpoints import get_BQ_df
from core.treintaLogger import get_logger

logger = get_logger('Bitrix ETL', "DEBUG")

def etl_final(method_list, method_get, table_id):
    ################################################################
    #Input
    df = get_method_list(method_list)
    df_endpoint = get_by_id(df, method_get)
    if get_BQ_df(table_id, df_endpoint) == False:
        response_BQ = dataframe_to_BQ(df_endpoint, table_id)
        logger.debug(F"{response_BQ}")
        return logger.debug('Tabla {} procesada'.format(table_id))
    else:
        df_BQ, df_BQ_max = get_BQ_df(table_id, df_endpoint)
        
        if df_BQ.empty:
            return logger.debug('Tabla {} sin registros nuevos'.format(table_id))
        else:
            columns_valid = ['DATE_MODIFY', 'DATE_REGISTER', 'LAST_UPDATED']

            columna_found = None
            for columna in columns_valid:
                try:
                    if columna in df_endpoint.columns:
                        columna_found = columna
                        break
                except KeyError:
                    pass
            if columna_found is not None:
                df_endpoint = df_endpoint[df_endpoint[columna_found] > df_BQ_max['max_date_modify'][0]]
            
            merged = df_endpoint.merge(df_BQ, on='ID', how='outer', indicator=True, suffixes=('', '_bq'))
            print(merged)
            ################################################################
            #Update
            update_data = merged[merged['_merge'] == 'both']
            for columna in columns_valid:
                try:
                    if columna in update_data.columns:
                        columna_found = columna
                        break
                except KeyError:
                    pass
            if columna_found is not None:
                update_data = update_data[update_data[columna_found] != update_data[columna_found + '_bq']]
                    
            update_data = update_data[df_endpoint.columns]

            if not update_data.empty:
                logger.debug("Hay datos por actualizar")
                response_BQ = update_to_BQ(update_data, table_id)
                logger.debug(F"{response_BQ}")
                response_BQ = append_historic_to_BQ(update_data, table_id)
                logger.debug(F"{response_BQ}")
            else:
                logger.debug("Nada por actualizar") 
            ################################################################
            #Insert
            new_data = merged[merged['_merge'] == 'left_only']
            new_data = new_data[df_endpoint.columns]
            if not new_data.empty:
                logger.debug('Hay nuevos registros')
                response_BQ = append_to_BQ(new_data, table_id)
                logger.debug(F"{response_BQ}")
                response_BQ = append_historic_to_BQ(new_data, table_id)
                logger.debug(F"{response_BQ}")
                
            else:
                logger.debug("Nada por añadir") 
            ################################################################
            #Delete
            delete_data = merged[merged['_merge'] == 'right_only']
            delete_data = delete_data[df_endpoint.columns]
            if not delete_data.empty:
                print('Hay nuevos registros a borrar')
                print(delete_data)
                response_BQ = delete_to_BQ(delete_data, table_id)
                logger.debug(F"{response_BQ}")
            else:
                logger.debug("Nada por borrar") 

            return logger.debug('Tabla {} procesada \n'.format(table_id))

etl_final('crm.lead.userfield.list', 'crm.lead.userfield.get', 'treintaco-lz.bitrix.forms_response')
etl_final('crm.deal.userfield.list', 'crm.deal.userfield.get', 'treintaco-lz.bitrix.forms_response_deal')
etl_final('crm.deal.list', 'crm.deal.get', 'treintaco-lz.bitrix.deals')
#etl_final('user.get', 'user.get', 'treintaco-lz.bitrix.users')
etl_final('crm.contact.list', 'crm.contact.get', 'treintaco-lz.bitrix.clients')
etl_final('crm.company.list', 'crm.company.get', 'treintaco-lz.bitrix.companies')
etl_final('crm.lead.list', 'crm.lead.get', 'treintaco-lz.bitrix.leads')
etl_final('crm.activity.list', 'crm.activity.get', 'treintaco-lz.bitrix.activities')

