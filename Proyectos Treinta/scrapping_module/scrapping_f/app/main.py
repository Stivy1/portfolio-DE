from core.treintaLogger import get_logger
from services.bigquery import dataframe_to_BQ
from scrappers.scrap_f import scrapping_f


logger = get_logger('ScrapingBee', "DEBUG")

#Modulo DAN
logger.debug("Scrapping F")
df_f = scrapping_f(logger)
response_BQ_LZ = dataframe_to_BQ(df_f, "treintaco-lz.sku_sources.b2b_research_f", logger, "WRITE_APPEND")
logger.debug(F"{response_BQ_LZ}")
