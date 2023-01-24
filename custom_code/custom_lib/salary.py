import re
from custom_code.custom_lib.currency_api import CachedCurrencyAPI
import pandas as pd

def clean_salary_factory():
    currency_api = CachedCurrencyAPI()
    def clean(s: str):
        out = s.lower()
        number_k = out.count("k")

        is_hourly = False
        is_monthly = False
        is_dollar = False
        is_euro = False

        if number_k >= 1:
            out = out.replace("k", "000")

        if "hour" in out:
            is_hourly = True
        if "month" in out:
            is_monthly = True

        if "$" in out or "dollar" in out:
            is_dollar = True

        if "€" in out or "euro" in out:
            is_euro = True

        if is_hourly and is_monthly:
            raise Exception(f"Is it hourly on monthly salary?? {s}") 

        if is_dollar and is_euro:
            raise Exception(f"Is it in dollar or euro? {s}")

        numbers = re.findall("\d+", out)
        salary = -1
        if len(numbers) == 0:
            return float(salary)
        elif len(numbers) == 1:
            salary = float(numbers[0])
        # If it's a range, take average
        elif len(numbers) == 2:
            salary = (float(numbers[0]) + float(numbers[1])) / 2
        else:
            raise Exception(f"Unhandled case, 3 or more salary numbers? {s}")
        if is_dollar:
            salary_in_euros = currency_api.convert_usd_to_euros(salary)
        # Else assume it's euros
        else:
            salary_in_euros = salary
        
        if is_monthly:
            annual_salary_in_euros = salary_in_euros * 12
        elif is_hourly:
            # 8 hours / day, 20 days / month, 12 months / year
            annual_salary_in_euros = salary_in_euros * 8 * 20 * 12
        else:
            annual_salary_in_euros = salary_in_euros
        return annual_salary_in_euros
    return clean