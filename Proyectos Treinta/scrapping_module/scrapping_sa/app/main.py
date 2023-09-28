from core.treintaLogger import get_logger
from services.bigquery import dataframe_to_BQ
from scrappers.scrap_sa import scrapping_sa

logger = get_logger('ScrapingBee', "DEBUG")

#Modulo SA
logger.debug("Scrapping SA")

df_sa = scrapping_sa()
response_BQ_LZ = dataframe_to_BQ(df_sa, "treintaco-lz.sku_sources.b2b_research_sa", logger, "WRITE_APPEND")
logger.debug(F"{response_BQ_LZ}")
