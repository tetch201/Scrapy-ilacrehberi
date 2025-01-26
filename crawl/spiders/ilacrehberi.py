import scrapy
from scrapy import Request
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from fake_useragent import UserAgent
import pandas as pd
import logging
import re
import os

logging.basicConfig(filename='scrapy_log.log', level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
user_agent = UserAgent().random
data = []

colums = ['URL', 'Name', 'Update Date', 'Price', 'Data', 'Stuff', 'Categories1', 'Categories2', 'Categories3', 'Categories4', 'Categories5']
df = pd.DataFrame(columns=colums)
file_exists = os.path.isfile('data.xlsx')
if not file_exists:
    df.to_excel('data.xlsx', index=False, header=True)
    print("Data created.")

class IlacrehberiSpider(CrawlSpider):
    name = "ilacrehberi"
    allowed_domains = ["ilacrehberi.com"]
    start_urls = ["https://www.ilacrehberi.com/"]

    custom_settings = {
        'JOBDIR': 'state'
    }

    rules = [
        Rule(
        LinkExtractor(
            allow=(r'ilacrehberi.com'),
            deny_domains=(r'twitter.com', r'facebook.com')
            ),
            callback='parse',
            follow=True
        ),
    ]
    def parse(self, response):
        for link in response.css('a[href*="/v/"]'):
            yield response.follow(link, headers={'User-Agent': user_agent}, callback=self.parse_details)

    def parse_details(self, response):
        href_mod = response.url

        name = response.xpath('//h1/text()').get()
        name_mod = name.strip() if name else ''

        updata = response.xpath('//span/font/text()').get()
        updata_mod = re.sub(r'^.*?:', '', updata.strip()) if updata else ''
        if '|' in updata_mod:
            updata_mod = re.sub(r'^.*?\|', '', updata_mod.strip())


        price_mod = response.xpath('//tr/td[contains(., "TL  [")]/text()').get()
        price_mod = price_mod.strip() if price_mod else ''

        price_mod_price = 'N/A'
        price_mod_data = 'N/A'

        if price_mod:
            price_match = re.search(r'(\d+\.\d+)', price_mod)
            price_mod_price = price_match.group(0) if price_match else 'N/A'

            data_match = re.search(r'\[(.*?)\]', price_mod)
            price_mod_data = data_match.group(1) if data_match else 'N/A'

        td = response.xpath('//tr/td[contains(., "Etkin Madde")]/following-sibling::td/a/text()').get()
        stuff_mod = td.strip() if td else ''

        categories = response.xpath('//td[contains(., "İlaç Sınıfı")]/following-sibling::td/a/text()').getall()
        all_categories = [cat.strip() for cat in categories]

        data.append({
            'URL': href_mod,
            'Name': name_mod,
            'Update Date': updata_mod,
            'Price': price_mod_price,
            'Data': price_mod_data,
            'Stuff': stuff_mod,
            'Categories1': all_categories[0] if len(all_categories) > 0 else 'N/A',
            'Categories2': all_categories[1] if len(all_categories) > 1 else 'N/A',
            'Categories3': all_categories[2] if len(all_categories) > 2 else 'N/A',
            'Categories4': all_categories[3] if len(all_categories) > 3 else 'N/A',
            'Categories5': all_categories[4] if len(all_categories) > 4 else 'N/A'
        })

        if len(data) >= 1000:
            try:
                df = pd.DataFrame(data)
                with pd.ExcelWriter('data.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                    df.to_excel(writer, index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)
                    print("Data appended.")
                    data.clear()
                    print(data)
            except Exception as e:
                print("An error occurred:", str(e))

    def close(self):
        try:
            df = pd.DataFrame(data)
            with pd.ExcelWriter('data.xlsx', mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
                df.to_excel(writer, index=False, header=False, startrow=writer.sheets['Sheet1'].max_row)
                print("Data appended.")
        except Exception as e:
            print("An error occurred:", str(e))