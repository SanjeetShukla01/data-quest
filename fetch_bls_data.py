import os
import requests


BLS_BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
FILES = [
    "pr.class", "pr.contacts", "pr.data.0.Current", "pr.data.1.AllData",
    "pr.duration", "pr.footnote", "pr.measure", "pr.period", "pr.seasonal",
    "pr.sector", "pr.series", "pr.txt"]


HEADERS = {
    "User-Agent": "SanjeetShukla/1.0 (sanejets1900@gmail.com)"
}


os.makedirs("bls_data", exist_ok=True)


def download_file(file_name):
    url = BLS_BASE_URL + file_name
    local_path = os.path.join("bls_data", file_name)

    response = requests.get(url, headers=HEADERS)

    if response.status_code == 200:
        with open(local_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {file_name}")
    else:
        print(f"Failed to download {file_name}: {response.status_code}")


if __name__ == '__main__':
    for file in FILES:
        download_file(file)
