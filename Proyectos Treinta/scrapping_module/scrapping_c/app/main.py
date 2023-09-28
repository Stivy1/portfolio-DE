from core.treintaLogger import get_logger
from services.bigquery import dataframe_to_BQ
from scrappers.scrap_c import scrapping_c


logger = get_logger('ScrapingBee', "DEBUG")

#Modulo DAN
logger.debug("Scrapping CH")
df_c = scrapping_c(logger)
response_BQ_LZ = dataframe_to_BQ(df_c, "treintaco-lz.sku_sources.b2b_research_c", logger, "WRITE_APPEND")
logger.debug(F"{response_BQ_LZ}")
