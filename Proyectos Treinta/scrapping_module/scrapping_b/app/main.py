from core.treintaLogger import get_logger
from services.bigquery import dataframe_to_BQ
from scrappers.scrap_b import ScrapperBEES


logger = get_logger('ScrapingBee', "DEBUG")

#Modulo DAN
logger.debug("Scrapping BEES")
status, df_B = ScrapperBEES().run()
response_BQ_LZ = dataframe_to_BQ(df_B, "treintaco-lz.sku_sources.b2b_research_b", logger, "WRITE_APPEND")
logger.debug(F"{response_BQ_LZ}")
