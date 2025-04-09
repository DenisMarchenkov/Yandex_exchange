import ftplib
import os
import logging
import time
import datetime


def download_from_ftp(server, username, password, filename, local_directory, max_retries=3, retry_delay=5, remote_directory = "/"):
    # remote_directory = "/"  # Корневая директория на сервере

    attempt = 0
    while attempt < max_retries:
        try:
            with ftplib.FTP(server) as ftp:
                try:
                    ftp.login(user=username, passwd=password)
                    logging.info("Успешное подключение и авторизация на сервере")
                except ftplib.error_perm as e:
                    logging.error(f"Ошибка авторизации: {e}. Попытка {attempt + 1} из {max_retries}")
                    time.sleep(retry_delay)
                    attempt += 1
                    continue

                try:
                    ftp.cwd(remote_directory)
                    logging.info("Успешный переход в корневую директорию на сервере")
                except ftplib.error_perm as e:
                    logging.error(
                        f"Ошибка при переходе в директорию на сервере: {e}. Попытка {attempt + 1} из {max_retries}")
                    time.sleep(retry_delay)
                    attempt += 1
                    continue

                if not os.path.exists(local_directory):
                    os.makedirs(local_directory)

                files = ftp.nlst()
                if filename not in files:
                    logging.error(f"Файл {filename} не найден на сервере")
                    return

                local_path = os.path.join(local_directory, filename)

                for file_attempt in range(max_retries):
                    try:
                        with open(local_path, 'wb') as local_file:
                            ftp.retrbinary(f"RETR {filename}", local_file.write)
                            logging.info(f"Файл скачан: {filename}")
                        return
                    except (ftplib.all_errors, ConnectionError) as e:
                        logging.error(
                            f"Ошибка при скачивании {filename}: {e}. Попытка {file_attempt + 1} из {max_retries}")
                        time.sleep(retry_delay)

                logging.error(f"Не удалось скачать файл {filename} после {max_retries} попыток")
                return

        except (ftplib.all_errors, ConnectionError) as e:
            logging.error(f"Ошибка соединения с FTP-сервером: {e}. Попытка {attempt + 1} из {max_retries}")
            time.sleep(retry_delay)

        attempt += 1

    logging.error("Не удалось подключиться к FTP-серверу после нескольких попыток")



def copy_file_with_timestamp(src_file, dest_folder):
    """
    Копирует файл из одной папки в другую, добавляя к имени файла время копирования.

    :param src_file: Путь к исходному файлу
    :param dest_folder: Папка назначения
    """
    if not os.path.isfile(src_file):
        logging.error(f"Файл '{src_file}' не найден.")
        raise FileNotFoundError(f"Файл '{src_file}' не найден.")

    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
        logging.info(f"Папка назначения '{dest_folder}' создана.")

    # Получаем имя файла и его расширение
    base_name, ext = os.path.splitext(os.path.basename(src_file))

    # Генерируем метку времени
    timestamp = datetime.datetime.now().strftime("%d-%m-%Y_%H-%M-%S")

    # Формируем новое имя файла
    new_file_name = f"{base_name}_{timestamp}{ext}"
    dest_file = os.path.join(dest_folder, new_file_name)

    # Копируем файл вручную
    try:
        with open(src_file, 'rb') as src:  # Открываем исходный файл в режиме чтения бинарных данных
            with open(dest_file, 'wb') as dest:  # Открываем файл назначения в режиме записи бинарных данных
                while chunk := src.read(4096):  # Читаем данные из исходного файла порциями по 4096 байт
                    dest.write(chunk)  # Записываем каждую порцию данных в файл назначения
        logging.info(f"Файл '{src_file}' успешно скопирован в '{dest_file}'.")
    except Exception as e:
        logging.error(f"Ошибка при копировании файла: {e}")
        raise

    return dest_file