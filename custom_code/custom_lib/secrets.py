import os
from custom_code.custom_lib import settings


def load_secret(secret_name: str):
    secret_path = os.path.join(settings.SECRETS_PATH, secret_name)
    with open(secret_path, "r") as fp:
        secret = fp.read()
    return secret.rstrip()
