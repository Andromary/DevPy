import requests
import pandas as pd

from useful_func import get_key, get_cities_list, read_gz
from time import strftime, localtime
from dotenv import load_dotenv, find_dotenv

class CityWeather():
    """
    Класс для получения данных о погоде с сайта https://openweathermap.org/
    """
        
    def __init__(self, key: str, cities_list):
        self.__cities_list = cities_list
        # self.__key = self.__get_key()
        self.__key = key
        self.__url = "https://api.openweathermap.org/data/2.5/weather"

    def get_weather(self):
        """ получает по этому списку для каждого города данные о погоде
        API call: https://api.openweathermap.org/data/2.5/weather?q={city name}&appid={API key}
        """
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
        # print("=============", list(df_ts_city.columns))
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

    def load_csv_gz(self, file_name: str):
        """выгрузить в csv, сжать через gzip"""
        # имя файла с расширением .csv.gz
        df = self.transform()
        df.to_csv((file_name + '.csv.gz'), compression='gzip')




"""
Структура API https://openweathermap.org/
{
  "coord": {
    "lon": 10.99,
    "lat": 44.34
  },
  "weather": [
    {
      "id": 501,
      "main": "Rain",
      "description": "moderate rain",
      "icon": "10d"
    }
  ],
  "base": "stations",
  "main": {
    "temp": 298.48,
    "feels_like": 298.74,
    "temp_min": 297.56,
    "temp_max": 300.05,
    "pressure": 1015,
    "humidity": 64,
    "sea_level": 1015,
    "grnd_level": 933
  },
  "visibility": 10000,
  "wind": {
    "speed": 0.62,
    "deg": 349,
    "gust": 1.18
  },
  "rain": {
    "1h": 3.16
  },
  "clouds": {
    "all": 100
  },
  "dt": 1661870592,
  "sys": {
    "type": 2,
    "id": 2075663,
    "country": "IT",
    "sunrise": 1661834187,
    "sunset": 1661882248
  },
  "timezone": 7200,
  "id": 3163858,
  "name": "Zocca",
  "cod": 200
}
"""