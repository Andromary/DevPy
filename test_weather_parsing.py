import pytest
from useful_func import get_key, get_cities_list, read_gz
from weather_parsing import CityWeather

@pytest.fixture()
def get_k():
    key_name = 'OW_TOKEN'
    api_key = get_key(key_name)
    return api_key

@pytest.fixture()
def get_c_l():
    file_name = 'cities'
    city_list = get_cities_list(file_name)
    return city_list


def test_get_weather(get_k, get_c_l):
    req_weather = CityWeather(get_k, get_c_l)
    resp = req_weather.get_weather()
    # pprint(resp)
    assert resp != []

def test_transform(get_k, get_c_l):
    req_weather = CityWeather(get_k, get_c_l)
    df = req_weather.transform()
    col_list = ['base', 'visibility', 'dt', 'timezone', 'id', 'name', 'cod', 'coord.lon', 'coord.lat', 'main.temp', 'main.feels_like', 'main.temp_min', 'main.temp_max', 'main.pressure', 'main.humidity', 'main.sea_level', 'main.grnd_level', 'wind.speed', 'wind.deg', 'wind.gust', 'clouds.all', 'sys.type', 'sys.id', 'sys.country', 'sys.sunrise', 'sys.sunset', 'rain.1h', 'weather.id', 'weather.main', 'weather.description', 'weather.icon', 'timestamp', 'city']
    df_col = list(df.columns)
    assert df_col == col_list and df['name'][1] == df['city'][1] and df['timestamp'][0] 


