import math
import time
import logging
import requests
import pandas as pd

from settings import *


class FakeResponse:
    def __init__(self, status_code=200, json_data=None):
        self.status_code = status_code
        self._json = json_data or {"message": "Тестовый ответ"}
        self.text = "Тестовый ответ"
        self.headers = {"Retry-After": 7}

    def json(self):
        return self._json


def prepare_offers_data(file_products, file_markup):
    """
    Читает данные из двух файлов XLS, рассчитывает новые цены с учетом наценки и формирует список для update_prices.

    :param file_products: str - Путь к первому файлу (содержит колонки: 'Артикул', 'Price', 'Mark').
    :param file_markup: str - Путь ко второму файлу (содержит колонки: 'Mark', 'MarkUP').
    :return: list - Список словарей с обновленными ценами.
    """
    try:
        # Загружаем данные
        logging.info("Загружаем данные из файлов: %s и %s", file_products, file_markup)
        df_products = pd.read_excel(file_products)
        df_markups = pd.read_excel(file_markup)

        # Проверяем наличие нужных столбцов
        required_columns_1 = {"Артикул", "Price", "Mark"}
        required_columns_2 = {"Mark", "MarkUP"}

        logging.info("Проверяем наличие нужных столбцов в файлах.")
        if not required_columns_1.issubset(df_products.columns) or not required_columns_2.issubset(df_markups.columns):
            logging.error("Один из файлов не содержит нужных столбцов.")
            raise ValueError("Один из файлов не содержит нужных столбцов")

        # Объединяем таблицы по полю 'Mark'
        logging.info("Объединяем данные по столбцу 'Mark'.")
        df = df_products.merge(df_markups, on="Mark", how="left")

        # Заполняем NaN в MarkUP единицами (чтобы не менять цену, если наценки нет)
        logging.info("Заполняем пропуски в 'MarkUP' значением 1.8.")
        df["MarkUP"] = df["MarkUP"].fillna(1.8)

        # Рассчитываем новую цену
        logging.info("Рассчитываем новые цены с учетом наценки.")
        df["NewPrice"] = df["Price"] * df["MarkUP"]
        markup_yandex = df_markups[df_markups['Mark'] == 'YANDEX_old_price']['MarkUP'].values[0] # Получаем наценку по марке YANDEX_old_price
        df["DiscountPrice"] = df["NewPrice"] * markup_yandex

        # Формируем список офферов
        logging.info("Формируем список офферов.")

        offers = []
        for _, row in df.iterrows():
            offer_id = str(row["Артикул"])
            price = math.ceil(row["NewPrice"])
            discount_base = math.ceil(row["DiscountPrice"])

            logging.info(f"Processing offer: offerId={offer_id}, price={price}, discountBase={discount_base}")

            offers.append({
                "offerId": offer_id,
                "price": price,
                "discountBase": discount_base
            })

        logging.info("Генерация списка офферов завершена.")
        return offers

    except Exception as e:
        logging.error("Произошла ошибка при обработке данных: %s", e)
        raise


def prepare_batches(offers, batch_size=300):
    """
    Разбивает список товаров на партии и формирует JSON-структуру.

    :param offers: list - Список товаров [{"offerId": str, "price": int, "discountBase": int}, ...]
    :param batch_size: int - Максимальное количество товаров в одном запросе.
    :return: list - Список JSON-объектов для API.
    """
    batches = []
    for i in range(0, len(offers), batch_size):
        batch = offers[i:i + batch_size]
        batch_json = {
            "offers": [
                {
                    "offerId": offer["offerId"],
                    "price": {
                        "value": offer["price"],
                        "currencyId": "RUR",
                        "discountBase": offer["discountBase"]
                    }
                }
                for offer in batch
            ]
        }
        batches.append(batch_json)
    return batches


def send_request_with_retries(url, headers, body, max_attempts=3):
    """
    Отправляет запрос с повтором при временных ошибках.

    :param url: str - URL API.
    :param headers: dict - Заголовки запроса.
    :param body: dict - Тело запроса.
    :param max_attempts: int - Максимальное число попыток.
    :return: dict | None - Ответ API или None в случае ошибки.
    """
    attempt = 0

    while attempt < max_attempts:
        try:
            #response = requests.post(url, headers=headers, json=body)
            response = FakeResponse()

            # ✅ Если всё ок, возвращаем результат
            if response.status_code == 200:
                return response.json()

            # 🔄 Ошибки 500-599 — повторяем
            elif 500 <= response.status_code < 600:
                logging.warning(f"Ошибка сервера ({response.status_code}). Попытка {attempt + 1} из {max_attempts}.")
                attempt += 1
                time.sleep(3)

            # ⏳ Ошибка 429 — ждём, если сервер сказал сколько
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                logging.warning(f"Слишком много запросов. Ждём {retry_after} сек.")
                time.sleep(retry_after)
                attempt += 1

            # 🚫 Ошибки 400-499 (кроме 429) — не повторяем
            else:
                logging.error(f"Ошибка API: {response.status_code} - {response.text}")
                return None

        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка сети ({e}). Попытка {attempt + 1} из {max_attempts}.")
            attempt += 1
            time.sleep(3)

    return None  # Если после всех попыток не удалось отправить


def update_prices(api_token, business_id, offers):
    """
    Обновляет цены на маркетплейсе Яндекса партиями.

    :param api_token: str - API ключ.
    :param business_id: int - ID бизнеса.
    :param offers: list - Список товаров.
    :return: list - Ответы API по партиям.
    """
    url = f"https://api.partner.market.yandex.ru/businesses/{business_id}/offer-prices/updates"
    headers = {
        'Api-Key': api_token,
        'Accept': 'application/json',
        'X-Market-Integration': 'OrderGuardTest'
    }

    responses = []
    batches = prepare_batches(offers)

    logging.info(f"Начинаем обновление цен для {len(offers)} товаров ({len(batches)} партий).")

    for idx, batch in enumerate(batches, start=1):
        response = send_request_with_retries(url, headers, batch)

        if response:
            logging.info(f"Партия {idx}/{len(batches)} успешно обновлена.")
        else:
            logging.error(f"Ошибка при обновлении партии {idx}/{len(batches)}.")

        responses.append(response)
        time.sleep(1)  # Задержка между запросами

    logging.info("Обновление цен завершено.")
    return responses



def start_exchange_price():
    offers = prepare_offers_data(OFFERS_FILE, MARKUP_FILE)
    update_prices(API_TOKEN, BUSINESS_ID, offers)


if __name__ == "__main__":
    start_exchange_price()
