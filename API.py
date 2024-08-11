import requests
import json


def get_url():
    api_url = "https://b2b.itresume.ru/api/statistics"
    client = 'Skillfactory'
    client_key = 'M2MGWS'
    start_date = '2023-01-30'
    end_date = '2023-01-31'
    params = {
        'client': client,
        'client_key': client_key,
        'start': start_date,
        'end': end_date
        }

    try:
        # Проверяет на наличие ошибок HTTP (например, 404, 500)
        response = requests.get(api_url, params=params)
        response.raise_for_status()

        # Пытаемся преобразовать ответ в JSON
        try:
            gets = response.json()
        except ValueError:
            print("Ошибка: Неверный формат ответа JSON.")
            return None

        return gets
    except requests.exceptions.HTTPError as err:
        print(f'HTTP Error: {err}')
    except requests.exceptions.RequestException as err:
        print(f'Request Error: {err}')

    return None


data = get_url()

json_data = json.dumps(data, indent=4, ensure_ascii=False)

print(json_data)
