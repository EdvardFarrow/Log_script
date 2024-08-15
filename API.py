import requests
import json
import pandas as pd
import psycopg2
import logging
import numpy as np
import os
import datetime
import shutil
import configparser

# Функция для удаления старых логов
def clean_old_logs(log_dir, days=3):
    cutoff_date = datetime.datetime.now() - datetime.timedelta(days=days)
    for filename in os.listdir(log_dir):
        file_path = os.path.join(log_dir, filename)
        if os.path.isfile(file_path):
            file_date_str = filename.split('.')[0]
            try:
                file_date = datetime.datetime.strptime(file_date_str, '%Y-%m-%d')
                if file_date < cutoff_date:
                    os.remove(file_path)
                    logging.info(f'Удален старый лог: {file_path}')
            except ValueError:
                logging.warning(f'Пропущен файл с неправильным именем: {file_path}')


# Чтение конфигурационного файла
config = configparser.ConfigParser()
config.read('config.ini')

# Настройка логирования
log_dir = 'logs'
os.makedirs(log_dir, exist_ok=True)
clean_old_logs(log_dir)

log_filename = datetime.datetime.now().strftime('%Y-%m-%d') + '.log'
log_path = os.path.join(log_dir, log_filename)


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)


def get_url():
    api_url = config['API']['api_url']
    params = {
        'client': config['API']['client'],
        'client_key': config['API']['client_key'],
        'start': config['API']['start_date'],
        'end': config['API']['end_date']
    }

    logging.info('Скачивание данных из API началось')
    try:
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        try:
            gets = response.json()
            logging.info('Скачивание данных завершилось успешно')
        except ValueError:
            logging.error("Ошибка: Неверный формат ответа JSON.")
            return None

        return gets
    except requests.exceptions.HTTPError as err:
        logging.error(f'HTTP Error: {err}')
    except requests.exceptions.RequestException as err:
        logging.error(f'Request Error: {err}')

    return None


def expand_passback_params(params):
    if params is None:
        return {}
    try:
        params_dict = json.loads(params.replace("'", '"'))
    except json.JSONDecodeError:
        logging.error(f"Ошибка: Неверный формат JSON в passback_params: {params}")
        params_dict = {}
    return params_dict


def process_boolean(value):
    if pd.isna(value):
        return None
    return bool(value)


logging.info('Получение данных началось')
data = get_url()

logging.info('Заполнение DataFrame началось')
if data is not None and isinstance(data, list):
    records = []
    for record in data:
        if "passback_params" in record:
            passback_params = expand_passback_params(record["passback_params"])
            expanded_record = {
                "user_id": record.get("lti_user_id", ""),
                "oauth_consumer_key": passback_params.get("oauth_consumer_key", ""),
                "lis_result_sourcedid": passback_params.get("lis_result_sourcedid", ""),
                "lis_outcome_service_url": passback_params.get("lis_outcome_service_url", ""),
                "is_correct": record.get("is_correct", ""),
                "attempt_type": record.get("attempt_type", ""),
                "created_at": record.get("created_at", "")
            }
            records.append(expanded_record)
    df = pd.DataFrame(records)
    df['is_correct'] = df['is_correct'].apply(process_boolean)
    logging.info('DataFrame успешно заполнен')
else:
    logging.warning('Не удалось получить или преобразовать данные')
    df = pd.DataFrame()

logging.info('Вставка данных в базу данных началась')
try:
    conn = psycopg2.connect(
        dbname=config['DATABASE']['dbname'],
        user=config['DATABASE']['user'],
        password=config['DATABASE']['password'],
        host=config['DATABASE']['host'],
        port=config['DATABASE']['port']
    )
    cur = conn.cursor()

    # Создание таблицы, если её нет
    create_table_query = """
    CREATE TABLE IF NOT EXISTS records (
        user_id TEXT,
        oauth_consumer_key TEXT,
        lis_result_sourcedid TEXT,
        lis_outcome_service_url TEXT,
        is_correct BOOLEAN,
        attempt_type TEXT,
        created_at TIMESTAMP
    );
    """
    cur.execute(create_table_query)
    conn.commit()
    logging.info('Таблица создана или уже существует')

    if df is not None and not df.empty:
        insert_query = """
        INSERT INTO records (
            user_id,
            oauth_consumer_key,
            lis_result_sourcedid,
            lis_outcome_service_url,
            is_correct,
            attempt_type,
            created_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        records_to_insert = df.values.tolist()
        cur.executemany(insert_query, records_to_insert)
        conn.commit()
        logging.info(f'{len(records_to_insert)} записей успешно вставлено в базу данных')
    else:
        logging.warning('DataFrame пуст или не был создан')

except Exception as e:
    logging.error(f"Ошибка при работе с PostgreSQL: {e}")

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()
    logging.info('Соединение с базой данных закрыто')
