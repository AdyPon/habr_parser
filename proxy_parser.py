import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


def proxy_parser():
    proxies = []

    headers = {
        'accept': '*/*',
        'user-agent': UserAgent().chrome
    }

    req = requests.get('https://hidemy.name/ru/proxy-list/?type=5&anon=4#list', headers=headers)

    soup = BeautifulSoup(req.content, 'lxml')

    table = soup.find('div', class_='table_block').find('tbody')

    for row in table:
        tags = row.findAll('td')
        proxies.append(f'socks5://{tags[0].text.strip()}:{tags[1].text.strip()}')

    return proxies
