from core.treintaLogger import get_logger
from services.bigquery import dataframe_to_BQ
from scrappers.scrap_ca import scrapping_ca


logger = get_logger('ScrapingBee', "DEBUG")

#Modulo CA
logger.debug("Scrapping CA")
df_ln = scrapping_ca(logger)
response_BQ_LZ = dataframe_to_BQ(df_ln, "treintaco-lz.sku_sources.b2b_research_ca", logger, "WRITE_APPEND")
logger.debug(F"{response_BQ_LZ}")
