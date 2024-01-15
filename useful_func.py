"""Модуль содержит вспомогательные функции"""
import os
import pandas as pd
import json
from dotenv import load_dotenv, find_dotenv


def get_key(token_name: str) -> str:
        """Получает значение ключа из переменной окружения"""
        load_dotenv(find_dotenv())
        api_key = os.environ.get(token_name)
        return api_key

def get_cities_list(file_name) -> list:
    """загружается из файла/конфига список городов для которых нужно получить данные о погоде """
    with open(file_name, "rb") as f:
        cit = f.read()
        cities = json.loads(cit)
    return cities

def read_gz(file_name: str):
    """Чтение данных из архива"""
    df = pd.read_csv(file_name, compression='gzip')
    return df