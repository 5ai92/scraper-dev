from scrape import Scraper
import logging
import psycopg2
from psycopg2.extensions import AsIs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Tollywood(Scraper):
    def __init__(self, host, path):
        self.host = host
        self.path = Scraper.options(path)
        super().__init__(self.host)
        return

    @property
    def _href(self):
        lookup_tables = self.set_tables_to_be_scraped("Director")
        titles = {}
        total_data = []
        tables = self.get_obj('table')
        if len(tables) > 1:
            for num, table in enumerate(tables):
                if num in lookup_tables:
                    for title in table.find_all('i'):
                        try:
                            if len(title.find_all('a', href=True)) > 0:
                                for every_movie in title.find_all('a', href=True):
                                    data = {}
                                    data['title'] = every_movie.text
                                    data['poster_title'] = every_movie['title']
                                    data['href'] = every_movie['href']
                                    titles[every_movie['title']] = every_movie['href']
                                    total_data.append(data)

                        except Exception as e:
                            logger.warning("We have an href exception here {}".format(str(e)))
                            continue
        yield total_data

    def get_details(self):
        titles = self._href
        # Now, we will have to use every path to scrape all the details from the request
        for title, link in titles.items():
            try:
                if link.find('edit') > 0:
                    continue
                else:
                    self.path = Scraper.options(titles[title])
                    super().__init__(self.host)
                    tables = self.get_obj('table')
                    if tables is None:
                        return

                    # identifying tables to be scraped
                    todo_table = int(self.set_tables_to_be_scraped("Directed by")[0])
                    rows = tables[todo_table].find_all('tr')
                    data = {}
                    for every_row in rows:
                        for every_header, every_td in zip(every_row.find_all('th'), every_row.find_all('td')):
                            data.update({every_header.get_text().strip().replace('\n', ''):
                                             every_td.get_text().strip().replace('\n', ',')})
                    data.update({"title": title})

                    poster_url = self.get_posters(self.soup, self.host)
                    if poster_url is not None:
                        pass

                    # if poster_url is not None:
                    #     cloudinary.uploader.upload(poster_url, public_id ="posters/{}".format(str(title) + "_poster" ))
                    #     logger.info("Uploaded image successfully to cloud")
                    # else:
                    #     pass
                    if len(data) > 0:
                        yield data
            except Exception as e:
                logger.debug("There is an exception here in get_details {}".format(str(e)))
                continue

    @staticmethod
    def get_posters(soup, host):
        imgs = soup.find_all('img')
        for each_image in imgs:
            if "png" in each_image['src']:
                continue
            elif "jpg" in each_image['src']:
                img_src = each_image['src']
                return "https:" + str(img_src)
            else:
                return None

    def _get_cast_plot(self):
        pass

    def get_movie_details(self):
        movie_data = (each_data_point for each_data_point in self._href)
        for each_year in movie_data:
            for each_movie in each_year:
                print(each_movie)
                if each_movie['href'].find('edit') < 0:
                    self.path = self.options(each_movie['href'])
                    super().__init__(self.host)
                    all_tables = self.get_obj('table')

                    if all_tables is not None:
                        lookup_table = int(self.set_tables_to_be_scraped("Directed by")[0])
                        rows = all_tables[lookup_table].find_all('tr')
                        for every_row in rows:
                            for every_header, every_td in zip(every_row.find_all('th'), every_row.find_all('td')):
                                each_movie.update({
                                    every_header.get_text().strip().replace('\n', ''): every_td.get_text().strip().replace('\n', ',')
                                })
                                each_movie.update({'poster_public_id': "posters/{}".format(str(each_movie['poster_title'])) + "_poster"})
                            yield each_movie


class LoadToPostgres:

    """ This object helps us to read and load csv into postgres database table """

    def __init__(self, host, dbname, user, password):
        self.conn_string = "host='{host}' dbname= '{dbname}' user= '{user}' password= '{password}'".format(
            host=host,
            dbname=dbname,
            user=user,
            password=password
        )
        self.conn = psycopg2.connect(self.conn_string)
        self.cursor = self.conn.cursor()

    def insert_query(self, query, columns, values):
        """ To insert a dict of in an order of keys-table headers and values-table rows"""
        insert_query = self.cursor.mogrify(query, (AsIs(','.join(columns)), tuple(values)))
        self.cursor.execute(insert_query)
        self.conn.commit()
        return


if __name__ == "__main__":

    for year in range(1940, 2018):
        try:
            data_list = Tollywood("https://en.wikipedia.org",
                                  "/wiki/List_of_Telugu_films_of_{}".format(year)).get_movie_details()
            for data in data_list:
                if data is not None:
                    data.update({'release_year': year})
                    cols = data.keys()
                    cols_refined = [(each_item.lower()).replace(' ', '_') for each_item in cols]
                    vals = [data[col] for col in cols]
                else:
                    pass
                    # logger.debug("loaded data")

        except Exception as e:
            # logger.warning("Error ==========================================" + str(e))
            pass
