import cloudscraper, random
import json
from add_offenders import clean_offenders, insert_offenders
import time
import pandas as pd

def get_next_proxy():
    global proxy_index
    proxy = proxies_list[proxy_index]
    proxy_index = (proxy_index + 1) % len(proxies_list)
    return proxy

def load_proxies(file_path):
    with open(file_path, "r") as f:
        proxies = f.read().splitlines()
    return proxies

proxies_list = load_proxies("proxies.txt")
proxy_index = 0
retry_zips = []

def get_offenders(zip_arr):
    proxies = {
        'http': get_next_proxy()
    }
    search_url = "https://nsopw-api.ojp.gov/nsopw/v1/v1.0/search"

    search_headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        "Referer": "https://www.nsopw.gov/",
        "sec-ch-ua": "\"Not)A;Brand\";v=\"99\", \"Brave\";v=\"127\", \"Chromium\";v=\"127\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Linux\"",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json; charset=UTF-8",
        "Origin": "https://www.nsopw.gov",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "cross-site",
        "Sec-GPC": "1"
    }

    search_data = {
        "firstName": "",
        "lastName": "",
        "city": "",
        "county": "",
        "zips": zip_arr,
        "clientIp": ""
    }

    scraper = cloudscraper.create_scraper()
    try:
        response = scraper.post(search_url, headers=search_headers, json=search_data, proxies=proxies)

        if response.status_code == 200:
            content_type = response.headers.get('Content-Type')
            if content_type and 'application/json' in content_type:
                try:
                    data = response.json()
                    if 'offenders' in data:
                        offenders = clean_offenders(data['offenders'])
                        print(f'done with offenders {zip_arr}')
                        return offenders
                    else:
                        print(f"No offenders data for zip {zip_arr}")
                        return None
                except json.JSONDecodeError:
                    print(f"Error decoding JSON for zip {zip_arr}: {response.text}")
            else:
                print(f"Unexpected content type: {content_type}")
                print(f"Response text: {response.text}")

        elif response.status_code == 429:
            time.sleep(3)
            return get_offenders(zip_arr)
        else:
            print(f"Search failed with status code: {response.status_code} for zip {zip_arr}")
            print(f"Response text: {response.text}")
            return get_offenders(zip_arr)
    except Exception as exc:
        print(f"An error occurred: {exc} for zip {zip_arr}")

    return None