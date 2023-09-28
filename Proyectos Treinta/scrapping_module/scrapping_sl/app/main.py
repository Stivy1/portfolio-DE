from core.treintaLogger import get_logger
from services.bigquery import dataframe_to_BQ
from scrappers.scrap_sl import scrapper_sl

logger = get_logger('ScrapingBee', "DEBUG")

#Modulo SA
logger.debug("Scrapping SL")

df_sl = scrapper_sl()
response_BQ_LZ = dataframe_to_BQ(df_sl, "treintaco-lz.sku_sources.b2b_research_sl", logger, "WRITE_TRUNCATE")
logger.debug(F"{response_BQ_LZ}")
