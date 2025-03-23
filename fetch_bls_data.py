import os
import requests


BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
USER_AGENT = "MyApp/1.0 (contact: your-email@example.com)"
DOWNLOAD_DIR = "bls_data"


os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def get_file_list():
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(BASE_URL, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch file list (Status Code: {response.status_code})")

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.text, "html.parser")
    files = [a.text for a in soup.find_all("a") if not a.text.endswith("/")]  # Exclude directories
    return files[1::]


def download_file(url, file_name):
    headers = {"User-Agent": USER_AGENT}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        with open(os.path.join(DOWNLOAD_DIR, file_name), "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {file_name}")
    else:
        print(f"Failed to download: {file_name} (Status Code: {response.status_code})")


def main():
    files = get_file_list()
    for file_name in files:
        file_url = BASE_URL + file_name
        download_file(file_url, file_name)


if __name__ == '__main__':
    main()
