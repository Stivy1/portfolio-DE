from core.treintaLogger import get_logger
from services.bigquery import dataframe_to_BQ
from scrappers.scrap_dan import ScrapperDAN


logger = get_logger('ScrapingBee', "DEBUG")

#Modulo DAN
logger.debug("Scrapping DAN")
status, df_dan = ScrapperDAN().run()
response_BQ_LZ = dataframe_to_BQ(df_dan, "treintaco-lz.sku_sources.b2b_research_dan", logger, "WRITE_APPEND")
logger.debug(F"{response_BQ_LZ}")
