import requests
import json
import csv
import os
import threading
import sqlite3
from dataclasses import dataclass, asdict, astuple
from fake_useragent import UserAgent
from bs4 import BeautifulSoup


@dataclass
class DropShoping:
    name: str | None
    avalaible_item: str | None
    price: str | None


def request_url(url):
    ua = UserAgent()
    headers = {'headers': ua.random}
    response = requests.get(url, headers=headers)
    try:
        if response.status_code == 200:
            return response
        else:
            raise Exception
    except Exception as e:
        print(f'Exception {e}')


def get_next_page(response):
    try:
        soup = BeautifulSoup(response.content, 'html5lib')
        next_urls = soup.select_one('div.page-index > a.next')
        next_url = next_urls['href']
        abs_url = f'https://cjdropshipping.com{next_url}'
        print(abs_url)
        response = request_url(abs_url)
        return scrape(response)
    except Exception as e:
        print('Error', str(e))
        return 0


def extract_text(soup, tag, sel):
    try:
        text = soup.find(tag, sel).text.strip()
        return text
    except:
        return 0


def extract_text_two(soup, sel):
    try:
        text = soup.select_one(sel).text.strip()
        return text
    except:
        return 0


def scrape(response):
    result = []
    result2 = []
    soup = BeautifulSoup(response.content, 'html5lib')
    box = soup.findAll('div', class_='card-wrap')
    for item in box:
        datas = DropShoping(
            name=extract_text(item, 'a', {'class': 'desc'}),
            avalaible_item=extract_text_two(item, 'div.list-collect > span > div > span:nth-child(2)'),
            price=extract_text(item, 'div', {'class': 'price'})
        )
        result.append(asdict(datas))
        result2.append(astuple(datas))
    # print(result)
    writer_to_json(result)
    writer_to_csv(result)
    sql_writer(result2)
    get_next_page(response)


def writer_to_json(data):
    path = 'shipping'
    if os.path.isfile(path):
        with open(f'{path}.json', 'r') as file:
            char = json.load(file)
        char.append(data)
        with open(f'{path}.json', 'w', encoding='utf-8') as file:
            json.dump(char, file, indent=2)
    else:
        with open(f'{path}.json', 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=2)


def writer_to_csv(data):
    paths = 'shipping'
    # file_exists = os.path.isfile(paths)
    field_name = list(data[0].keys())
    with open(f'{paths}.csv', 'a',  newline='', encoding='utf-8') as csv_file:
        pen = csv.DictWriter(csv_file, fieldnames=field_name)
        csv_file.seek(0, 2)
        if csv_file.tell() == 0:
            pen.writeheader()
        # if not file_exists:
        #    pen.writeheader()
        pen.writerows(data)


def sql_writer(data):
    conn = sqlite3.connect('shipping.db')
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS scraped_data (name TXT, available REAL, price REAL)")
    cur.executemany('INSERT INTO scraped_data VALUES (?, ?, ?)', data)
    cur.execute('SELECT * FROM scraped_data')
    da = cur.fetchall()
    for ro in da:
        print(ro)


def main():
    urls = 'https://cjdropshipping.com/search/ps4.html'
    html = request_url(urls)
    scrape(html)


if __name__ == '__main__':
    t1 = threading.Thread(target=main)
    t1.start()
    t1.join()
