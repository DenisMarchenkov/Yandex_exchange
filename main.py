import logging
import os.path

from settings import *
from utils import download_from_ftp, copy_file_with_timestamp
from stocks import start_exchange_stock
from prices import start_exchange_price

logging.basicConfig(
    filename=f"{BASE_DIR}/exchange.log",  # Файл для логов
    level=logging.INFO,            # Уровень логирования
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


def main():
    download_from_ftp(SERVER, USER_NAME, PASSWORD, STOCK_PRICE_FILE, SUPPLIER_DIR)
    start_exchange_stock()
    start_exchange_price()
    copy_file_with_timestamp(os.path.join(SUPPLIER_DIR, STOCK_PRICE_FILE), SUPPLIER_DIR_COMPLETED)

if __name__ == '__main__':
    main()
