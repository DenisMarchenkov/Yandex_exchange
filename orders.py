import ftplib
import os
import requests
import dbf

from pprint import pprint
from datetime import datetime
from settings import CAMPAIGN_ID, API_TOKEN, CUSTOMER_ID_IN_SUPPLIER_CRM, DIVISION_ID, ORDERS_DIR, SERVER, USER_NAME, PASSWORD, STORE


def get_orders(campaign_id, api_token, limit=20, page_token=None, offer_ids=None):
    url = f"https://api.partner.market.yandex.ru/campaigns/{campaign_id}/orders"

    headers = {
        'Api-Key': api_token,
        'Accept': 'application/json',
        'X-Market-Integration': 'OrderGuardTest'
    }

    params = {
        "limit": limit,
        "fake": 'false',
        "status": "PROCESSING",
        "substatus": "STARTED",
        #"orderIds": "43013054787",
    }

    if page_token:
        params["page_token"] = page_token

    if offer_ids:
        if isinstance(offer_ids, list):
            params["offerId"] = ",".join(offer_ids)
        else:
            params["offerId"] = offer_ids

    all_orders = []

    while True:
        try:
            response = requests.get(url, headers=headers, params=params)

            if response.status_code != 200:
                print(f"Ошибка при запросе: {response.status_code}")
                print(response.text)
                break

            data = response.json()

            # Список заказов лежит в data["orders"]
            orders = data.get("orders", [])
            all_orders.extend(orders)

            # Пейджинг в data["paging"]
            next_page_token = data.get("paging", {}).get("nextPageToken")

            if not next_page_token:
                break

            params["page_token"] = next_page_token

        except Exception as e:
            print(f"Ошибка при получении заказов: {e}")
            break

    return all_orders

def extract_order_data(orders):
    result = []

    for order in orders:
        order_id = order.get("id")
        items = order.get("items", [])
        result.append({
            "order_id": order_id,
            "status": order.get("status"),
            "substatus": order.get("substatus"),
            "creationDate": order.get("creationDate"),
            "shipmentDate": order.get("delivery", {}).get("shipments", [])[0].get("shipmentDate"),
        })

    return result

def export_orders_to_dbf_files(orders, output_dir='dbf_orders'):
    os.makedirs(output_dir, exist_ok=True)

    for order in orders:
        order_id = order.get("id")
        order_date_str = order.get("creationDate")
        shipment_data = order.get("delivery", {}).get("shipments", [])
        items = order.get("items", [])

        if not items:
            continue

        # Парсим дату заказа
        try:
            order_date = datetime.strptime(order_date_str, "%d-%m-%Y %H:%M:%S").date()
        except:
            order_date = None

        # Парсим дату отгрузки, если есть
        try:
            shipment_date_str = shipment_data[0].get("shipmentDate") if shipment_data else None
            shipment_date = datetime.strptime(shipment_date_str, "%d-%m-%Y").date() if shipment_date_str else None
        except:
            shipment_date = None

        filename = os.path.join(output_dir, f"{order_id}.dbf")

        if os.path.exists(filename):
            print(f"Файл уже существует, пропускаю: {filename}")
            continue

        table = dbf.Table(
            filename,
            'offer_id C(50); name C(255); price N(10,2); qty N(10,0); post_num C(50); '
            'ord_date D; ship_date D; comment C(255); cust_id C(10); div_id C(10)',
            codepage='cp866'
        )
        table.open(mode=dbf.READ_WRITE)

        for item in items:
            table.append((
                item.get("offerId", ""),
                item.get("offerName", ""),
                float(item.get("price", 0)),
                int(item.get("count", 0)),
                str(order_id),
                order_date,
                shipment_date,
                f'{order_id} Заказ YANDEX Frenchpharmacy',
                str(CUSTOMER_ID_IN_SUPPLIER_CRM),
                str(DIVISION_ID)
            ))

        table.close()

        upload_file_to_ftp(server=SERVER, username=USER_NAME, password=PASSWORD,
                           store=STORE, local_path_file= filename)


def upload_file_to_ftp(server, username, password, store, local_path_file):
    remote_directory = f'/YANDEX_orders/{store}/Orders/'
    # remote_directory_additional = f'/YANDEX_prices/{store}/Orders'  # Дополнительная папка для учета остатков

    try:
        with ftplib.FTP(server) as ftp:
            try:
                ftp.login(user=username, passwd=password)
                #logging.info("Успешное подключение и авторизация на сервере")
                print("Успешное подключение и авторизация на сервере")
            except ftplib.error_perm as e:
                #logging.error(f"Ошибка авторизации: {e}")
                print(f"Ошибка авторизации: {e}")
                return

            filename = os.path.basename(local_path_file)

            # Функция для загрузки в указанную директорию
            def upload_to_directory(directory):
                try:
                    ftp.cwd(directory)
                    #logging.info(f"Успешный переход в директорию {directory}")
                    print(f"Успешный переход в директорию {directory}")
                except ftplib.error_perm:
                    #logging.warning(f"Директория {directory} не найдена, создаем...")
                    print(f"Директория {directory} не найдена, создаем...")
                    try:
                        ftp.mkd(directory)
                        ftp.cwd(directory)
                        #logging.info(f"Директория {directory} создана и выбрана")
                        print(f"Директория {directory} создана и выбрана")
                    except ftplib.error_perm as e:
                        #logging.error(f"Ошибка при создании директории {directory}: {e}")
                        print(f"Ошибка при создании директории {directory}: {e}")
                        return

                # Загружаем файл
                try:
                    with open(local_path_file, 'rb') as file:
                        ftp.storbinary(f'STOR {filename}', file)
                        #logging.info(f"Файл загружен: {filename} в {directory}")
                        print(f"Файл загружен: {filename} в {directory}")
                except ftplib.all_errors as e:
                    #logging.error(f"Ошибка при загрузке файла {filename} в {directory}: {e}")
                    print(f"Ошибка при загрузке файла {filename} в {directory}: {e}")

            # Загружаем в основную папку
            upload_to_directory(remote_directory)

            # Загружаем в дополнительную папку для учета остатков
            # upload_to_directory(remote_directory_additional)

    except ftplib.all_errors as e:
        #logging.error(f"Ошибка соединения с FTP-сервером: {e}")
        print(f"Ошибка соединения с FTP-сервером: {e}")


def main():
    orders = get_orders(CAMPAIGN_ID, API_TOKEN)
    data_orders = extract_order_data(orders)

    export_orders_to_dbf_files(orders, output_dir=ORDERS_DIR)

if __name__ == '__main__':
    main()

