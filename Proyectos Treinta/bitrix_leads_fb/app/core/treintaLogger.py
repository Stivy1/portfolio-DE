import logging


def get_level(log_level: str):
    if log_level == "DEBUG":
        return logging.DEBUG
    elif log_level == "INFO":
        return logging.INFO
    elif log_level == "WARNING":
        return logging.WARNING
    elif log_level == "ERROR":
        return logging.ERROR
    elif log_level == "CRITICAL":
        return logging.CRITICAL
    else:
        return logging.INFO


def get_logger(log_name: str, log_level: str):

    logger = logging.getLogger(log_name)
    level = get_level(log_level.upper())

    c_handler = logging.StreamHandler()
    c_handler.setLevel(level)
    c_format = logging.Formatter('%(asctime)s [%(name)s] [%(levelname)s]: %(message)s')
    c_handler.setFormatter(c_format)

    logger.addHandler(c_handler)
    logger.setLevel(level)

    return logger
