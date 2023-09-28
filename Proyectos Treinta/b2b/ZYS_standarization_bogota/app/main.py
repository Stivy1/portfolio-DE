from models.product import TropiProduct
from models.order import TropiOrder
from models.combos import TropiCombos
from services.db import get_last_batch
from services.bigquery import dataframe_to_BQ
from services.drive import text_to_drive
from datetime import datetime
from core.treintaLogger import get_logger
import pandas as pd
logger = get_logger('ZyS Orders', "DEBUG")

CONSEC_LEN = 7
HEADER = '00000001001'
FOOTER = '99990001001'

current_date = str(pd.Timestamp.now(tz='America/Bogota').strftime('%Y-%m-%d %H-%M-%S'))
text = '{}.txt'.format(current_date)
df = get_last_batch(logger)

if df.empty is True:
    logger.info("No hay datos para subir")
    exit()
else:
    pass

tropi_total_lines = []
for i, order in df.iterrows():

    tropi_products = [
        TropiProduct(
            F_431_ID_CONSEC_DOCTO=order['F_431_CONSEC_DOCTO'],
            F_431_NRO_REGISTRO=idx,
            F_431_ID_ITEM=product['F_431_ID_ITEM'],
            F_431_FECHA_ENTREGA=order['F_430_FECHA_ENTREGA'],
            F_431_ID_UNIDAD_MEDIDA=product['F_431_ID_UNIDAD_MEDIDA'],
            F_431_CANT_PEDIDA_BASE=product['F_431_CANT_PEDIDA_BASE'])
        for idx, product in zip(
            range(1, len(order['PRODUCTS']) + 1), order['PRODUCTS'])
            if product['F_431_ID_UNIDAD_MEDIDA'] != "9910001"
        ]

    tropi_combos = [
        TropiCombos(
            F_431_ID_CONSEC_DOCTO=order['F_431_CONSEC_DOCTO'],
            F_431_FECHA_ENTREGA=order['F_430_FECHA_ENTREGA'],
            F_431_CODIGO_PAQUETE=product['F_431_ID_ITEM'],
            F_431_CANT_PAQUETE=product['F_431_CANT_PEDIDA_BASE'])
        for idx, product in zip(
            range(1, len(order['PRODUCTS']) + 1), order['PRODUCTS'])
            if product['F_431_ID_UNIDAD_MEDIDA'] == "9910001"
        ]

    tropi_order = TropiOrder(
        F_430_CONSEC_DOCTO=order['F_431_CONSEC_DOCTO'],
        F_430_ID_FECHA=order['F_430_ID_FECHA'],
        F_430_ID_TERCERO_FACT=str(order["F_430_ID_TERCERO_FACT"]),
        F_430_ID_SUCURSAL_FACT=str(order["F_430_ID_SUCURSAL_FACT"]),
        F_430_ID_TERCERO_REM=str(order["F_430_ID_TERCERO_FACT"]),
        F_430_ID_SUCURSAL_REM=str(order["F_430_ID_SUCURSAL_FACT"]),
        F_430_FECHA_ENTREGA=order['F_430_FECHA_ENTREGA'],
        F_430_NUM_DOCTO_REFERENCIA=str(order['F_430_NUM_DOCTO_REFERENCIA']),
        F_430_NOTAS=str(order['F_430_NOTAS']),
        TROPI_PRODUCTS=tropi_products,
        TROPI_COMBOS=tropi_combos)

    tropi_total_lines.extend(tropi_order.get_lines())

tropi_total_lines = [HEADER] + tropi_total_lines + [FOOTER]
n_lines = len(tropi_total_lines)
main_consecutive = [f'{i}'.zfill(CONSEC_LEN) for i in range(1, n_lines+1)]

full_lines = [''.join(lines)
              for lines in zip(main_consecutive, tropi_total_lines)]
full_text = '\n'.join(full_lines)

with open(text, 'w') as f:
    f.write(full_text)

fichero = open(text, 'r')
texto_completo = fichero.read()
caracteres = len(texto_completo)
if caracteres >= 38:
    logger.debug("Archivo construido con exito!!")
    response_BQ = dataframe_to_BQ(df, "data-production-337318.b2b_zys.orders_uploaded_bogota", logger)
    logger.debug(F"{response_BQ}")
    response_drive = text_to_drive(text, "1FeHkzktCE5KXfWjz0uOtfxe_aJKzBcDv")
    logger.debug(F"{response_drive}")
    print("Ejecuto")
else:
    logger.warning("No se pudo construir el archivo =(")
    exit()
