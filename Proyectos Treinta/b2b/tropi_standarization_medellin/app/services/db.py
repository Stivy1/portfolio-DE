import pandas as pd
from google.cloud import bigquery
import google.auth
from datetime import datetime
from holidays_co import is_holiday_date
from datetime import timedelta
import json
from google.oauth2 import service_account
import random


#from oauth2client.service_account import ServiceAccountCredentials

def next_day(days: int) -> datetime:
    next_day = datetime.today() + timedelta(days=days)
    return next_day


def get_last_batch(logger: object):
  #USAR PARA DESPLIEGUES
    credentials, project = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/bigquery"
        ])
    #print(credentials.__dict__)
    #print(project) 
    client = bigquery.Client(credentials=credentials, project=project)
    #USAR PARA PRUEBAS LOCALES
    # path = json.load(open('tropi_credentials.json'))
    # credentials = service_account.Credentials.from_service_account_info(path, scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/bigquery"])
    
    # #print(credentials.__dict__)
    # client = bigquery.Client(credentials=credentials, project = credentials.project_id)

    week_day = datetime.today().weekday()
    #LOgica Standar 
    post_day = 1
    if week_day == 5 and is_holiday_date(next_day(2)) is True:
        post_day = 3
    elif week_day == 5 and is_holiday_date(next_day(2)) is False:
        post_day = 2
    elif week_day == 4 and is_holiday_date(next_day(1)) is True:
        post_day = 3
    elif (week_day != 5 or week_day != 6) and is_holiday_date(next_day(1)) is True:
        post_day = 2
    elif week_day == 6:
        print("!!Es domingo :D!!")
        exit()
    else:
        post_day = 1

    #Logica Temporal
    # post_day = 0
    # if week_day == 4:
    #     post_day = 3
    # elif week_day == 5:
    #     post_day = 3
    # elif week_day == 6:
    #     print("!!Es domingo :D!!")
    #     exit()
    # else:
    #     post_day = 2

    rand = random.randint(0, 99)

    query = """SELECT 
                DISTINCT od.order_id AS ORDER_ID,
                o.external_id AS F_431_CONSEC_DOCTO,
                cast(o.document AS INTEGER) AS F_430_ID_TERCERO_FACT,
                sucursal AS F_430_ID_SUCURSAL_FACT,
                DATE(order_date) AS F_430_ID_FECHA,
                DATE(delivery_date) AS F_430_FECHA_ENTREGA,
                od.quantity AS F_431_CANT_PEDIDA_BASE,
                pr.sku AS F_431_ID_ITEM,
                p.external_id AS F_431_ID_UNIDAD_MEDIDA,
                SUBSTR(CONCAT("T.V ", o.vendor_phone, " ", o.discount),1,54) AS F_430_NOTAS,
                codigo_vendedor AS F_430_ID_TERCERO_VENDEDOR,
                o.cia AS F_430_ID_CO,
                o.fact AS F_430_ID_CO_FACT,
                o.cia AS F_431_ID_CO,
                o.bodega AS F_431_ID_BODEGA,
                o.movto AS F_431_ID_CO_MOVTO
            FROM (
                SELECT
                      o.id AS order_id,
                      o.external_id,
                      COALESCE(effective_delivery_date, delivery_date) AS delivery_date,
                      CURRENT_DATETIME('America/Bogota') AS order_date,
                      tdoc.document_clean AS document,
                      IFNULL(TRANSLATE(UPPER(sa.additional_information) , 'ÁÉÍÓÚÑ', 'AEIOUN'), "") AS additional_information,
                      IFNULL(TRANSLATE(UPPER(sa.neighborhood) , 'ÁÉÍÓÚÑ', 'AEIOUN'), "") AS neighborhood,
                      IFNULL(CAST(v.phone AS STRING), "") AS vendor_phone,
                      CASE WHEN o.applied_discount != 0 THEN CONCAT("DESCUENTO: ",applied_discount) ELSE "NO APLICA DESCUENTO" END AS discount,
                      CASE WHEN supplier_id = '1a894222-6a8f-471a-b944-4f415fe85e7f' THEN 571 else 571 END as sucursal,
                      CASE WHEN supplier_id = '1a894222-6a8f-471a-b944-4f415fe85e7f' then "1701" else "1702" end as codigo_vendedor,
                      CASE WHEN supplier_id = '1a894222-6a8f-471a-b944-4f415fe85e7f' then "020" else "020" end as cia,
                      CASE WHEN supplier_id = '1a894222-6a8f-471a-b944-4f415fe85e7f' then "020" else "020" end as fact,
                      CASE WHEN supplier_id = '1a894222-6a8f-471a-b944-4f415fe85e7f' then "411" else "411" end as bodega,
                      CASE WHEN supplier_id = '1a894222-6a8f-471a-b944-4f415fe85e7f' then "020" else "020" end as movto,
                  FROM `treintaco-lz.treintagp_b2b.orders` o
                  LEFT JOIN `treintaco-lz.treintagp_b2b.stores_invoicing_information` si 
                    ON si.id = o.store_invoicing_information_id
                 LEFT JOIN `treintaco-lz.treintagp_b2b.store_addresses` sa
                    ON sa.id = o.store_address_id
                  INNER JOIN (
                    SELECT 
                      b.document_b2b,
                      a.document_clean,
                      a.sucursal 
                    FROM `treintaco-sandbox.b2b_tropi.tropi_documents_clean_medellin` a
                    INNER JOIN `treintaco-sandbox.b2b_tropi.new_tropi_upload_documents` b
                    ON a.document_clean = CAST(b.document_clean AS STRING)
                  ) tdoc
                    ON tdoc.document_b2b = TRIM(si.document)
                  INNER JOIN `treintaco-lz.treintagp_b2b.orders_group` og ON o.order_group_id = og.id
                  LEFT JOIN `treintaco-sz.sales.agents` v
                  ON CAST(v.id AS STRING) = o.salesman_code
                  WHERE supplier_id in ('1a894222-6a8f-471a-b944-4f415fe85e7f')
                  AND o.id NOT IN (SELECT order_id FROM `treintaco-sandbox.b2b_tropi.orders_uploaded_medellin`)
                  AND COALESCE(effective_delivery_date, delivery_date) = CURRENT_DATE("-5:00") + {0}
                  AND o.status_id IN (1,7)
                  AND ((COALESCE(og.payment_type, "OFFLINE") = "OFFLINE") OR (COALESCE(og.payment_type, "ONLINE") = "ONLINE" AND COALESCE(o.payment_status, "COMPLETED") = "COMPLETED"))
                )o
            INNER JOIN `treintaco-lz.treintagp_b2b.order_details` od ON o.order_id = od.order_id
            INNER JOIN `treintaco-lz.treintagp_b2b.warehouses_products` p ON p.id = od.warehouse_product_id
            INNER JOIN `treintaco-lz.treintagp_b2b.products` pr ON pr.id = p.product_id
            WHERE od._fivetran_deleted is false
            AND p._fivetran_deleted is false
            AND pr._fivetran_deleted is false
            AND p.deleted_at is null
            AND pr.deleted_at is null
            ORDER BY 4 DESC""".format(str(post_day))
    query_job = client.query(query)
    df = query_job.to_dataframe()

    if df.empty is True:
        logger.info(df)
        logger.warning("No hay datos para extraer")
        return pd.DataFrame()
    else:
        df['F_430_ID_FECHA'] = pd.to_datetime(df['F_430_ID_FECHA'])
        df['F_430_FECHA_ENTREGA'] = pd.to_datetime(df['F_430_FECHA_ENTREGA'])
        df['F_430_ID_FECHA'] = df['F_430_ID_FECHA'].dt.strftime('%Y%m%d')
        df['F_430_FECHA_ENTREGA'] = df['F_430_FECHA_ENTREGA'].dt.strftime('%Y%m%d')
        df['F_431_CONSEC_DOCTO'] = pd.to_numeric(df['F_431_CONSEC_DOCTO'])
        df['F_430_ID_SUCURSAL_FACT'] = pd.to_numeric(df['F_430_ID_SUCURSAL_FACT'])
        df['F_430_ID_FECHA'] = pd.to_numeric(df['F_430_ID_FECHA'])
        df['F_430_FECHA_ENTREGA'] = pd.to_numeric(df['F_430_FECHA_ENTREGA'])
        df['F_431_CANT_PEDIDA_BASE'] = pd.to_numeric(df['F_431_CANT_PEDIDA_BASE'])
        #df['F_431_ID_ITEM'] = pd.to_numeric(df['F_431_ID_ITEM'])
        df['PRODUCTS'] = df[['F_431_CANT_PEDIDA_BASE', 'F_431_ID_ITEM',
                            'F_431_ID_UNIDAD_MEDIDA', 'F_431_ID_BODEGA',
                            'F_431_ID_CO', 'F_431_ID_CO_MOVTO']].to_dict(orient='records')

        df_agg = df.groupby('ORDER_ID').agg(
                            F_431_CONSEC_DOCTO=('F_431_CONSEC_DOCTO', 'min'),
                            F_430_ID_SUCURSAL_FACT=('F_430_ID_SUCURSAL_FACT', 'min'),
                            F_430_ID_TERCERO_FACT=('F_430_ID_TERCERO_FACT', 'min'),
                            F_430_ID_FECHA=('F_430_ID_FECHA', 'min'),
                            F_430_FECHA_ENTREGA=('F_430_FECHA_ENTREGA', 'min'),
                            F_430_NOTAS=('F_430_NOTAS', 'min'),
                            F_430_ID_TERCERO_VENDEDOR=('F_430_ID_TERCERO_VENDEDOR', 'min'),
                            F_430_ID_CO=('F_430_ID_CO', 'min'),
                            F_430_ID_CO_FACT = ('F_430_ID_CO_FACT', 'min'),
                            PRODUCTS=('PRODUCTS', list))
        df_agg.reset_index(inplace=True)
        #Para evitar duplicados
        duplicates = df_agg[df_agg.duplicated(subset=['F_431_CONSEC_DOCTO'], keep='first')]
        df_agg.loc[duplicates.index,'F_431_CONSEC_DOCTO'] = df_agg.loc[duplicates.index,'F_431_CONSEC_DOCTO']+1
    return df_agg
