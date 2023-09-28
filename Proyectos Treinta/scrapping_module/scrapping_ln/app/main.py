from core.treintaLogger import get_logger
from services.bigquery import dataframe_to_BQ
from scrappers.scrap_ln import scrapping_ln


logger = get_logger('ScrapingBee', "DEBUG")

#Modulo LN
logger.debug("Scrapping LN")
df_ln = scrapping_ln(logger)
response_BQ_LZ = dataframe_to_BQ(df_ln, "treintaco-lz.sku_sources.b2b_research_ln", logger, "WRITE_APPEND")
logger.debug(F"{response_BQ_LZ}")
