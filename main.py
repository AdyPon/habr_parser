import pandas as pd
from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
import csv
from pathlib import Path
from itertools import cycle
from proxy_parser import proxy_parser
import asyncio
import aiohttp
from aiohttp_socks import ProxyConnector
from aiohttp_retry import RetryClient, ExponentialRetry


class HabrParser:
    def __init__(self, urls_file: str, proxies: list, limit=None):
        """
        urls_file (str): файл с ссылками на профили
        proxies (list): список прокси
        limit (int): ограничение количества ссылок
        """
        self.links = pd.read_csv(urls_file)
        if limit:
            self.links = self.links.iloc[:limit]
        self.proxies = cycle(proxies)

    def start(self, continue_parsing=False, successful_links='successful_links.csv') -> None:
        """
        Запускает парсинг.
        continue_parsing (bool): True - если парсинг был прерван и его нужно продолжить,
                                 False - если парсинг нужно начать заново.
        successful_links (str): Файл с ссылками, которые не нужно использовать.
        """
        print('Parsing has started!')
        asyncio.get_event_loop().run_until_complete(self.get_all_pages(continue_parsing, successful_links))
        print('Parsing is finished!')

    async def get_page_handler(self, link, session, pbar):
        try:
            await asyncio.sleep(0.1)
            retry_options = ExponentialRetry(attempts=5, max_timeout=2)
            retry_client = RetryClient(session)
            async with retry_client.get(link, retry_options=retry_options) as req:
                req_status = req.status
                if req_status == 200:
                    src = await req.text()
                    soup = BeautifulSoup(src, 'lxml')

                    user_name = soup.find('h1', class_='page-title__title').text.strip()

                    new_user_name = user_name
                    for i in [' ', '*']:
                        new_user_name = new_user_name.replace(i, '_')

                    path = Path('pages', new_user_name + '.html')

                    with open(path, 'w', encoding="utf-8") as file:
                        file.write(link + '\n')
                        file.write(user_name + '\n')
                        file.write(src)

                    with open('successful_links.csv', 'a', newline='') as csvfile:
                        successful_links_writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                        successful_links_writer.writerow([link])

                    pbar.update(1)
                    return {'status_code': True, 'link': link}
                elif req_status == 404:
                    pbar.update(1)

                    with open('successful_links.csv', 'a', newline='') as csvfile:
                        successful_links_writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                        successful_links_writer.writerow([link])
                    return {'status_code': True, 'link': link}
                elif req_status == 429:
                    return {'status_code': False, 'link': link}
        except:
            return {'status_code': False, 'link': link}

    async def get_all_pages(self, continue_parsing: bool, successful_links: str) -> None:
        """
        Метод получения всех страниц.
        continue_parsing (bool): True - если парсинг был прерван и его нужно продолжить,
                                 False - если парсинг нужно начать заново.
        successful_links (str): Файл с ссылками, которые не нужно использовать.
        """
        try:
            Path('pages').mkdir()
        except FileExistsError:
            pass

        if continue_parsing:
            successful_links_df = pd.read_csv(successful_links)

            self.links.url = self.links.loc[~self.links.url.isin(successful_links_df.url)]
            self.links = self.links.dropna()

            self.links = self.links.url.to_list()
        else:
            self.links = self.links.url.to_list()

            with open('successful_links.csv', 'w', newline='') as csvfile:
                url_writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
                url_writer.writerow(['url'])

        links = set(self.links)
        with tqdm(total=len(links), desc='Parsing progress:') as pbar:
            count = 0

            while len(links) != 0:
                if count <= 50:
                    count += 1
                    proxy = next(self.proxies)
                    connector = ProxyConnector.from_url(proxy)
                    async with aiohttp.ClientSession(connector=connector) as session:
                        tasks = []
                        for link in links:
                            task = asyncio.create_task(self.get_page_handler(link, session, pbar))
                            tasks.append(task)

                        coroutine = await asyncio.gather(*tasks)

                    if len(coroutine) != len(links):
                        count = 0

                    for cor in coroutine:
                        if isinstance(cor, dict) and cor.get('status_code'):
                            links.remove(cor['link'])
                else:
                    print('The proxy has been changing for too long')
                    break


if __name__ == "__main__":
    my_proxies = ['socks5://w45L39:MV7Ue8@194.67.219.107:9750',
                  'socks5://testvicky2:2a31eb@193.23.50.202:10486',
                  'socks5://VSwLsQ:sEEUU3@194.28.208.64:9824']

    proxies = proxy_parser()

    parser = HabrParser('habr.csv', my_proxies + proxies)

    parser.start()
