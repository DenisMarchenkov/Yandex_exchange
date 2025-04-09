import csv
from pprint import pprint

import requests


from settings import BUSINESS_ID, API_TOKEN


def get_products(business_id, api_token, limit=20, page_token=None, offer_ids=None):
    url = f"https://api.partner.market.yandex.ru/businesses/{business_id}/offer-mappings"
    headers = {
        'Api-Key': f'{api_token}',
        'Accept': 'application/json',
        'X-Market-Integration': 'OrderGuardTest'
    }

    params = {
        "limit": limit
    }
    if page_token:
        params["page_token"] = page_token

    body = {}
    if offer_ids:
        body["offerIds"] = offer_ids


    response = requests.post(url, headers=headers, params=params, json=body)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Ошибка при получении списка товаров: {response.status_code} - {response.text}")
        return None



def get_all_campaign_offers(business_id, api_token, limit=20, page_token=None, offer_ids=None):
    """
    Получает полный список товаров из кампании, обрабатывая все страницы.

    :param api_token Экземпляр ApiClient.
    :param business_id: Идентификатор кампании.
    :param limit: Количество товаров на странице (по умолчанию 20).
    :param page_token: Токен следующей страницы для постраничного вывода.
    :param offer_ids: Список идентификаторов товаров для фильтрации (необязательно).
    :return: Список всех товаров.
    """
    url = f"https://api.partner.market.yandex.ru/businesses/{business_id}/offer-mappings"
    headers = {
        'Api-Key': f'{api_token}',
        'Accept': 'application/json',
        'X-Market-Integration': 'OrderGuardTest'
    }

    params = {
        "limit": limit
    }

    if page_token:
        params["page_token"] = page_token

    body = {}
    if offer_ids:
        body["offerIds"] = offer_ids

    all_offers = []
    while True:
        try:
            response = requests.post(url, headers=headers, params=params, json=body)

            # Проверяем статус ответа
            if response.status_code != 200:
                print(f"Ошибка при запросе: {response.status_code}")
                break

            data = response.json()

            # Добавляем товары из текущего ответа в общий список
            all_offers.extend(data['result']['offerMappings'])

            # Получаем токен следующей страницы
            next_page_token = data['result']['paging'].get('nextPageToken')

            # Если токена нет — больше страниц нет, выходим из цикла
            if not next_page_token:
                break

            # Обновляем page_token для следующего запроса
            params['page_token'] = next_page_token

        except Exception as e:
            print(f"Ошибка при получении списка товаров: {e}")
            break

    return all_offers


def save_to_csv_products(business_id, api_token, limit=20, page_token=None, offer_ids=None, filename="offers.csv"):
    """
    Получает полный список товаров из кампании и сохраняет в файл CSV.

    :param api_token: API-ключ.
    :param business_id: Идентификатор кампании.
    :param limit: Количество товаров на странице (по умолчанию 20).
    :param page_token: Токен следующей страницы для постраничного вывода.
    :param offer_ids: Список идентификаторов товаров для фильтрации (необязательно).
    :param filename: Имя файла для сохранения данных.
    :return: None
    """
    url = f"https://api.partner.market.yandex.ru/businesses/{business_id}/offer-mappings"
    headers = {
        'Api-Key': f'{api_token}',
        'Accept': 'application/json',
        'X-Market-Integration': 'OrderGuardTest'
    }
    params = {"limit": limit}
    if page_token:
        params["page_token"] = page_token

    body = {}
    if offer_ids:
        body["offerIds"] = offer_ids

    with open(filename, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=";")
        writer.writerow([
            'offerId', 'name', 'barcodes', 'basicPrice', 'vendor', 'vendorCode', 'category', 'manufacturerCountries', 'description'
        ])

        while True:
            try:
                response = requests.post(url, headers=headers, params=params, json=body)

                if response.status_code != 200:
                    print(f"Ошибка при запросе: {response.status_code}")
                    break

                data = response.json()

                for offer_data in data.get('result', {}).get('offerMappings', []):
                    offer = offer_data.get('offer', {})

                    writer.writerow([
                        offer.get('offerId', ''),
                        offer.get('name', ''),
                        ', '.join(offer.get('barcodes', [])),
                        offer.get('basicPrice', {}).get('value', ''),
                        offer.get('vendor', ''),
                        offer.get('vendorCode', ''),
                        offer.get('category', ''),
                        ', '.join(offer.get('manufacturerCountries', [])),
                        offer.get('description', '').replace("\n", " ")
                    ])

                next_page_token = data.get('result', {}).get('paging', {}).get('nextPageToken')
                if not next_page_token:
                    break

                params['page_token'] = next_page_token

            except Exception as e:
                print(f"Ошибка при получении списка товаров: {e}")
                break

    print(f"✅ Данные успешно сохранены в файл {filename}")


pprint(get_products(BUSINESS_ID, API_TOKEN, limit=1, offer_ids=['13458']))
#save_to_csv_products(BUSINESS_ID, API_TOKEN)