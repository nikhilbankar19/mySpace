# logging_example.py

import logging

f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

debuglogger = logging.getLogger("ETL_LOG")
f_handler = logging.FileHandler('D:/Catalog/JnJ/lens/JnJ-etl/lens_poc/data/etl_log.log')
f_handler.setLevel(logging.INFO)

f_handler.setFormatter(f_format)
debuglogger.addHandler(f_handler)
# Add handlers to the logger





#logger.warning('This is a warning')
#logger.error('This is an error')
