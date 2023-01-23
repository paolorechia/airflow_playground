import datetime as dt


def get_today_string() -> str:
    now = dt.datetime.now()
    return now.strftime("%Y-%m-%d")