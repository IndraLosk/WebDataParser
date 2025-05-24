import logging

logging.basicConfig(
    level=logging.INFO,
    filename="analytics.log",
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s",
)
