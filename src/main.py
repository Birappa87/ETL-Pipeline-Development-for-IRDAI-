from src.irdai_scraper import irdai_scraper_main
import logging

logging.basicConfig(
    filename="irdai_scraper.log",
    level=logging.DEBUG,
    format="%(asctime)s %(message)s",
    datefmt="%d-%b-%Y %H:%M:%S"
)

def main():
    try:
        logging.info("IRDAI Job started")
        irdai_scraper_main()
    except Exception as err:
        logging.info("IRDAI exception: {err}".format(err=err))
    finally:
        logging.info("IRDAI Job over")