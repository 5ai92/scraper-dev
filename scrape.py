from bs4 import BeautifulSoup
import requests
import pandas as pd
import logging


class Scraper:

    def __init__(self, host):
        self.host = host
        self.soup = self._get_soup(self.path)
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)

    @classmethod
    def options(cls, path):
        cls.path = path
        return cls.path

    def _get_soup(self, path):
        page = requests.get(self.host + path)
        if page.status_code == 200:
            soup = BeautifulSoup(page.content, 'lxml')
            return soup
        else:
            self.log.warning("There is an error in response from client")
            return

    def get_obj(self, obj):
        if self.soup.find_all(obj) is not None:
            return self.soup.find_all(obj)
        else:
            return

    @staticmethod
    def text(parent, todo):
        groups = getattr(parent, 'find_all("{}")'.format(todo))
        if groups is not None:
            for group in groups:
                yield group

    def get_table_data(self, table_number):
        """ We are using pandas to scrape data"""
        tables = pd.read_html(self.url, header=0)
        table = tables[table_number]
        return table

    def set_tables_to_be_scraped(self, key_word):
        tab_number = 0
        tables_todo = []
        tables = self.get_obj('table')
        for each_table in tables:
            tab_number += 1
            cols = each_table.find_all('th')
            for col in cols:
                if col.get_text() == key_word:
                    tables_todo.append(tab_number - 1)
        return tables_todo

    @staticmethod
    def load_csv(data_frame, outfile):
        return data_frame.to_csv(outfile, encoding='utf-8', index=False)

    @staticmethod
    def load_to_json(data_frame):
        return data_frame.to_json(orient='records')

