import requests
import json
from custom_code.custom_lib.date_utils import get_today_string
from custom_code.custom_lib.path_utils import get_today_currency, get_currency_fs
from custom_code.custom_lib.filesystem import DataFilesystem
from custom_code.custom_lib.secrets import load_secret

class CachedCurrencyAPI:
    _base_url = "https://api.apilayer.com/currency_data/convert?from=USD&to=EUR"

    def __init__(self) -> None:
        self._api_key = load_secret("apilayer.txt")

    def convert_usd_to_euros(self, usd: float):
        cache_fp = get_today_currency()
        fs: DataFilesystem = get_currency_fs()

        json = fs.read_json(cache_fp)

        if json:
            print("Cache hit, returning JSON from FS")
            rate = json["result"]

        else:
            print("Cache miss, fetching rate from API")
            url = CachedCurrencyAPI._base_url + f"&amount=1"
            url += "&date="
            url += get_today_string()

            print("Requesting url: ", url)
            response = requests.get(url, headers={
                "apikey": self._api_key
            })
            print(response.text)
            response.raise_for_status()
            j = response.json()
            fs.write_dict_as_json(cache_fp, j)
            rate = j["result"]

        return usd * rate