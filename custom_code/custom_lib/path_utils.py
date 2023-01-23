from custom_code.custom_lib.date_utils import get_today_string
from custom_code.custom_lib.filesystem import DataFilesystem

def get_today_remotive() -> str:
    today_ = get_today_string()
    fs = DataFilesystem("remotive")
    return fs.path(f"{today_}_remotive.json")


def get_currency_fs() -> str:
    return DataFilesystem("currency")


def get_today_currency() -> str:
    today_ = get_today_string()
    fs = get_currency_fs()
    return fs.path(f"{today_}_currency.json")
