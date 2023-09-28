import pandas as pd
from services.bigquery import dataframe_to_BQ
from services.db_bogota import get_last_batch_bog
from core.treintaLogger import get_logger
from services.drive import text_to_drive

logger = get_logger('Clients Creation', "DEBUG")
df_bog = get_last_batch_bog(logger)

current_date = str(pd.Timestamp.now(tz='America/Bogota').strftime('%Y-%m-%d %H-%M-%S'))
file = '{}.xlsx'.format(current_date)

if df_bog.empty is True:
    exit()
elif df_bog.empty is False:
    df_bog.to_excel(file, index=False)
    # response_drive = text_to_drive(file, "1lPsGrNplNfrPFHl12AVvRPI6IwJLwjS6")
    # logger.debug(F"{response_drive}")
    
    # response_BQ = dataframe_to_BQ(df_bog, "data-production-337318.b2b_ops.clients_uploaded", logger)
    # logger.debug(F"{response_BQ}")