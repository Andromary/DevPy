import os
import requests
import pandas as pd
import json
import csv
import gzip
from useful_func import get_key, get_cities_list
from time import strftime, localtime
from dotenv import load_dotenv, find_dotenv



# load_dotenv(find_dotenv())
# API_KEY = os.environ.get('OW_TOKEN')

# url = "https://api.openweathermap.org/data/2.5/weather"
# params = {
#     # "q": "city_name",
#     "q": "Москва",
#     "appid": API_KEY, 
# }
# resp = requests.get(url, params=params)
# resp = requests.get(url, q="Moscow", appid = API_KEY)

# print(resp)
# print(resp.text)

# https://api.openweathermap.org/data/2.5/weather?q={city name}&appid={API key}

class CityWeather():
    """
    Класс для получения данных о погоде с сайта https://openweathermap.org/
    """

    # def __init__(self, file_name: str): # когда всё было в одном классе
    #     self.__file_name = file_name
    #     # self.__key = self.__get_key()
    #     self.__key = key
    #     self.__url = "https://api.openweathermap.org/data/2.5/weather"
        
    def __init__(self, key: str, cities_list):
        self.__cities_list = cities_list
        # self.__key = self.__get_key()
        self.__key = key
        self.__url = "https://api.openweathermap.org/data/2.5/weather"

    # def __get_key(self): # когда всё было в одном классе
    #     """Получает значение ключа из переменной окружения"""
    #     load_dotenv(find_dotenv())
    #     api_key = os.environ.get('OW_TOKEN')
    #     return api_key

    # def __get_cities_list(self) -> list: # когда всё было в одном классе
    #     """загружается из файла/конфига список городов для которых нужно получить данные о погоде """
    #     with open(self.__file_name, "rb") as f:
    #         cit = f.read()
    #         cities = json.loads(cit)
    #         # print(cities)
    #     return cities

    def get_weather(self):
        """ получает по этому списку для каждого города данные о погоде"""
        city_weather_list = []
        # city_list = self.__get_cities_list() # когда всё было в одном классе
        city_list = self.__cities_list
        for city in city_list:
            # Задаем параметры, необходимые для доступа к данным по API
            params = {
                "q": city,
                "units": "metric",
                "appid": self.__key, 
            }
            # Передаем параметры запросы, получаем ответ
            try:
                response = requests.get(self.__url, params = params)
                city_weather_list.append(response.json())

                # print (city_weather_list)
            except Exception as e:
                print("Exception (find):", e)
                continue
        return city_weather_list

# ['{"coord":{"lon":37.6177,"lat":55.7507},"weather":[{"id":804,"main":"Clouds","description":"overcast clouds","icon":"04d"}],"base":"stations","main":{"temp":272.5,"feels_like":268.81,"temp_min":272.14,"temp_max":272.95,"pressure":988,"humidity":94,"sea_level":988,"grnd_level":971},"visibility":10000,"wind":{"speed":3.06,"deg":210,"gust":7.04},"clouds":{"all":100},"dt":1703414786,"sys":{"type":2,"id":2000314,"country":"RU","sunrise":1703397519,"sunset":1703422735},"timezone":10800,"id":524901,"name":"Moscow","cod":200}']

    def transform(self):
        """делает из полученного json с вложенными данными плоскую (широкую) таблицу"""
        # Получаем данные о погоде по API с сайта
        json_list = self.get_weather()

        # Преобразуем json в плоскую таблицу
        df = pd.json_normalize(json_list, record_prefix='weather.')

        # Избавляемся от списка в ключе 'weather'
        exploded_df = df.explode('weather',ignore_index = True)

        # Нормализуем данные с ключем 'weather'
        df_weather = pd.json_normalize(exploded_df['weather'])

        # Переименовываем названия столбцов
        new_col_names = {"id": "weather.id", "main": "weather.main", "description": "weather.description", "icon": "weather.icon"}
        rn_expl_df = df_weather.rename(columns = new_col_names)
        
        # Соединяем данные нормализованной таблицы без данных ключа 'weather' и отдельно нормализованных данных ключа 'weather'
        flat_df = pd.concat([exploded_df.drop('weather', axis = 1), rn_expl_df], axis = 1)
        # Добавляем столбец 'timestamp'
        df_ts = self.__add_timestamp(flat_df)

        # Добавляем столбец город
        df_ts_city = self.__add_city(df_ts)
        return df_ts_city


    def __add_timestamp(self, df):
        """Добавить столбец timestamp, когда были сняты данные о погоде"""
        # df = self.transform() 
        timestamp_data = []
        for i in range(len(df.axes[0])):
            # Получаем данные столбца 'dt'
            dt_data = df['dt']
            # Получаем данные для каждой строки
            # print('=',i,'=',dt_data[i])
            dt = dt_data[i]
            # Конвертируем дату из эпохального времени в обычный формат
            time = strftime('%Y-%m-%d %H:%M:%S', localtime(dt))
            timestamp_data.append(time)
        # 3. Вставить новую колонку 'timestamp' в df с полученным значением
        df.insert(len(df.columns) , 'timestamp', timestamp_data, True)
        # df['timestamp'] = timestamp_data # аналогично вышеуказанному
        return df

    def __add_city(self, df):
        """Добавить столбец город""" 
        # Столбец с названием города имеется в DataFrame в колонке с названием "name"
        # можно переименовать данный столбец, либо внести новый с тем же названием
        df['city'] = df['name']
        return df

    def load_csv_gz(self, df, file_name: str):
        """выгрузить в csv, сжать через gzip"""
        # имя файла с расширением .csv.gz
        df.to_csv((file_name + '.csv.gz'), compression='gzip')

    def read_gz(self, df, file_name: str):
        df = pd.read_csv(file_name, compression='gzip')
        return df

"""
+ загружается из файла/конфига список городов для которых нужно получить данные о погоде. Много не надо, штук 5
- получает по этому списку для каждого города данные о погоде
- делает из полученного json с вложенными данными плоскую (широкую) таблицу
- Добавить 2 столбца город и timestamp, когда были сняты данные о погоде
+ температура должна быть в цельсиях. Можно преобразовать, можно сразу через апи получить
- выгрузить в csv, сжать через gzip
- токена не должно быть видно в рабочем коде. Например, можно использовать https://pypi.org/project/python-dotenv/
"""

key_name = 'OW_TOKEN'
file_name = 'cities'
api_key = get_key(key_name)
city_list = get_cities_list(file_name)

# req_weather = CityWeather("cities") # Когда всё было в одном классе
req_weather = CityWeather(api_key, city_list)
# resp = req_weather.get_weather()
resp = req_weather.transform()

print(resp)



# {
#   "coord": {
#     "lon": 10.99,
#     "lat": 44.34
#   },
#   "weather": [
#     {
#       "id": 501,
#       "main": "Rain",
#       "description": "moderate rain",
#       "icon": "10d"
#     }
#   ],
#   "base": "stations",
#   "main": {
#     "temp": 298.48,
#     "feels_like": 298.74,
#     "temp_min": 297.56,
#     "temp_max": 300.05,
#     "pressure": 1015,
#     "humidity": 64,
#     "sea_level": 1015,
#     "grnd_level": 933
#   },
#   "visibility": 10000,
#   "wind": {
#     "speed": 0.62,
#     "deg": 349,
#     "gust": 1.18
#   },
#   "rain": {
#     "1h": 3.16
#   },
#   "clouds": {
#     "all": 100
#   },
#   "dt": 1661870592,
#   "sys": {
#     "type": 2,
#     "id": 2075663,
#     "country": "IT",
#     "sunrise": 1661834187,
#     "sunset": 1661882248
#   },
#   "timezone": 7200,
#   "id": 3163858,
#   "name": "Zocca",
#   "cod": 200
# }