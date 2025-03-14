import logging
import time
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



def prepare_offers_data(file_products):
    """
    Читает данные из основного файла XLS, выбирает количество товара и формирует список для update_prices.

    :param file_products: str - Путь к файлу (содержит колонки: 'Артикул', 'Количество').
    :return: list - Список словарей с обновленными данными.
    """
    try:
        # Загружаем основной файл
        logging.info("Загружаем данные из файла: %s", file_products)
        df_products = pd.read_excel(file_products)

        # Проверяем наличие нужных столбцов
        required_columns = {"Артикул", "Количество"}
        if not required_columns.issubset(df_products.columns):
            logging.error("Файл %s не содержит нужных столбцов.", file_products)
            raise ValueError(f"Файл {file_products} не содержит нужных столбцов")

        df_products['Артикул'] = df_products['Артикул'].astype(str)
        df_products['Количество'] = df_products['Количество'].fillna(0).astype(int)

        # Формируем список офферов
        logging.info("Формируем список офферов.")
        offers = []

        for _, row in df_products.iterrows():
            offer_id = row["Артикул"]
            count = row["Количество"]

            logging.info("Товар: Артикул=%s, Количество=%d", offer_id, count)

            offers.append({
                "offerId": offer_id,
                "qua": count,
            })

        logging.info("Генерация списка офферов завершена.")
        return offers

    except Exception as e:
        logging.error("Произошла ошибка при обработке данных: %s", e)
        raise



def prepare_batches(offers, batch_size=300):
    """
    Разбивает список товаров на партии и формирует JSON-структуру.

    :param offers: list - Список товаров [{"offerId": str, "qua": int}, ...]
    :param batch_size: int - Максимальное количество товаров в одном запросе.
    :return: list - Список JSON-объектов для API.
    """
    batches = []
    for i in range(0, len(offers), batch_size):
        batch = offers[i:i + batch_size]
        batch_json = {
            "skus": [
                {
                    "sku": offer["offerId"],  # Значение артикулов
                    "items": [
                        {
                            "count": offer["qua"],  # Количество товаров
                            #"updatedAt": "2022-12-29T18:02:01Z" # если не передаем то в яндексе устанавливается текущее время
                        }
                    ]
                }
                for offer in batch  # Генерация списка "skus"
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
            #response = requests.put(url, headers=headers, json=body)
            response = FakeResponse()

            # ✅ Если всё ок, возвращаем результат
            if response.status_code == 200:
                return response.json()

            # 🔄 Ошибки 500-599 — повторяем
            elif 500 <= response.status_code < 600:
                logging.warning(f"Ошибка сервера ({response.status_code} - {response.text}). Попытка {attempt + 1} из {max_attempts}.")
                print()
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



def update_stocks(api_token, campaign_id, offers):
    """
    Обновляет остатки на маркетплейсе Яндекса партиями.

    :param campaign_id: Идентификатор кампании в API и магазина в кабинете. Каждая кампания в API соответствует магазину в кабинете.
    :param api_token: str - API ключ.
    :param offers: list - Список товаров.
    :return: list - Ответы API по партиям.
    """

    url = f"https://api.partner.market.yandex.ru/campaigns/{campaign_id}/offers/stocks"
    headers = {
        'Api-Key': api_token,
        'Accept': 'application/json',
        'X-Market-Integration': 'OrderGuardTest'
    }

    responses = []
    batches = prepare_batches(offers)

    logging.info(f"Начинаем обновление остатков для {len(offers)} товаров ({len(batches)} партий).")

    for idx, batch in enumerate(batches, start=1):
        response = send_request_with_retries(url, headers, batch)

        if response:
            logging.info(f"Партия {idx}/{len(batches)} успешно обновлена.")
        else:
            logging.error(f"Ошибка при обновлении партии {idx}/{len(batches)}.")

        responses.append(response)
        time.sleep(1)  # Задержка между запросами

    logging.info("Обновление остатков завершено.")
    return responses



def start_exchange_stock():
    offers = prepare_offers_data(OFFERS_FILE)
    update_stocks(API_TOKEN, CAMPAIGN_ID, offers)


if __name__ == "__main__":
    start_exchange_stock()