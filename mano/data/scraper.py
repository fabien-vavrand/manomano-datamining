import os
import time
import logging
import requests
import threading
from lxml import html
from mano.data.utils import get_html_values


class ThreadedScraper(threading.Thread):

    def __init__(self, scraper, url, page):
        # Instanciate a new scraper to have a unique Session per thread
        threading.Thread.__init__(self)
        self.scraper = Scraper(scraper.url, scraper.save_path, scraper.max_pages)
        self.url = url
        self.page = page

    def run(self):
        self.status = self.scraper._try_scrap_sub_category(self.url, self.page)


class Scraper:

    def __init__(self, url, save_path=None, max_pages=None, n_threads=None):
        self.url = url
        self.save_path = save_path
        self.data_path = os.path.join(save_path, 'data')
        self.max_pages = max_pages or 1000
        self.timeout = 10
        self.sleep_time = 60
        self.max_tries = 5
        self.n_threads = n_threads
        self.session = requests.Session()

        if not os.path.exists(self.save_path):
            os.mkdir(self.save_path)

        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)

    def get_page(self, url):
        errors = 0
        while errors < self.max_tries:
            try:
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                page = html.fromstring(response.text)
                time.sleep(0.2)
                return response, page
            except requests.HTTPError as e:
                logging.warning('Page not found: {}'.format(url))
                return response, None
            except Exception:
                errors += 1
                time.sleep(self.sleep_time)
        raise ValueError('Connection error: {}'.format(url))

    def _is_scraping_finished(self, level, name):
        """ Function used to manage fast scraping recovery. Check if a category has been fully scraped """
        file_path = os.path.join(self.save_path, '{}.txt'.format(level))
        if not os.path.exists(file_path):
            return False
        with open(file_path, 'r') as file:
            categories = file.readlines()
            categories = [c.strip() for c in categories]
        return name in categories

    def _set_scraping_finished(self, level, name):
        """ Function used to manage fast scraping recovery. Save category once it is fully scraped """
        file_path = os.path.join(self.save_path, '{}.txt'.format(level))
        with open(file_path, 'a') as file:
            file.write(name + '\n')

    def run(self):
        r, page_html = self.get_page(self.url)
        group_categories = get_html_values(page_html, '//div/ul/li/ul/li/a/@href')

        for i, group_category in enumerate(group_categories):
            logging.info('{} ({}/{})'.format(group_category, i+1, len(group_categories)))
            if not self._is_scraping_finished('group_categories', group_category):
                self._scrap_group_category(group_category)
                self._set_scraping_finished('group_categories', group_category)

    def _scrap_group_category(self, url):
        """ Group categories are the second level categoties on the home page, like 'mobilier de jardin et jeux', 'piscine', .. """
        r, page_html = self.get_page(self.url + url)

        # Page not found
        if page_html is None:
            return

        categories = get_html_values(page_html, '//a[@data-qa=\"categoryLinkCta\"]/@href')

        for i, category in enumerate(categories):
            logging.info('  {}'.format(category))
            if not self._is_scraping_finished('categories', category):
                self._scrap_category(category)
                self._set_scraping_finished('categories', category)

    def _scrap_category(self, url):
        """ Categories are just below group categories, and are the lowest level visible in the home page menu, like 'Salon, table et chaise de jardin' """
        r, page_html = self.get_page(self.url + url)

        # Page not found
        if page_html is None:
            return

        sub_categories = get_html_values(page_html, '//a[@data-qa=\"filterLinkCta\"]/@href')

        for i, sub_category in enumerate(sub_categories):
            logging.info('    {}'.format(sub_category))

            if self._is_scraping_finished('sub_categories', sub_category):
                continue

            page = 1
            if self.n_threads is None:
                while self._try_scrap_sub_category(sub_category, page):
                    page += 1
            else:
                status = True
                while status:
                    n_threads = self.n_threads if page > 1 else 1
                    sessions = [ThreadedScraper(self, sub_category, page + i) for i in range(n_threads)]
                    [session.start() for session in sessions]
                    [session.join() for session in sessions]
                    status = all([session.status for session in sessions])
                    page += n_threads
            self._set_scraping_finished('sub_categories', sub_category)

    def _try_scrap_sub_category(self, url, page=1):
        """ When scraping failed on a page, we skip it by returning True and continue to the next page """
        try:
            return self._scrap_sub_category(url, page)
        except Exception as e:
            logging.error(str(e))
            return False

    def _scrap_sub_category(self, url, page=1):
        """ Sub categories correspond to filters on categories """
        if page > self.max_pages:
            return False

        page_path = os.path.join(self.data_path, '{}-page-{}.json'.format(url[1:], page))
        if os.path.exists(page_path):
            return False

        r, page_html = self.get_page(self.url + url + ('?page={}'.format(page) if page > 1 else ''))

        # Page not found
        if page_html is None:
            return False

        if self._has_no_product(page_html):
            return False

        # Deal with category redirecting, which can lead to an infinite loop
        if url not in r.url:
            return False

        logging.info('      page {}'.format(page))
        results = str(page_html.xpath('//div[@id=\"fragment-listing\"]/script/text()'))
        self._save_sub_category(page_path, results)
        return True

    def _save_sub_category(self, page_path, results):
        with open(page_path, 'w', encoding='utf-8') as file:
            file.write(results)

    def _has_no_product(self, page_html):
        """ not used. Initially for scraping products from html"""
        no_result_div = page_html.xpath('//div[@class=\"products-no-results\"]')
        return len(no_result_div) > 0
