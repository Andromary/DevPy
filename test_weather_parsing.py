import pytest
import pprint
from weather_parsing import CityWeather

def test_get_weather():
    req_weather = CityWeather("cities")
    resp = req_weather.get_weather()
    pprint(resp)
    assert False
