from core.treintaLogger import get_logger
from services.bigquery import dataframe_to_BQ
from scrappers.scrap_te import scrapping_te


logger = get_logger('ScrapingBee', "DEBUG")

#Modulo LN
logger.debug("Scrapping TE")
df_te = scrapping_te(logger)
response_BQ_LZ = dataframe_to_BQ(df_te, "treintaco-lz.sku_sources.b2b_research_te", logger, "WRITE_APPEND")
logger.debug(F"{response_BQ_LZ}")
