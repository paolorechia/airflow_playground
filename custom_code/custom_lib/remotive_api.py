import requests

def get_json():
    response = requests.get("https://remotive.com/api/remote-jobs")
    return response.json()["jobs"]
