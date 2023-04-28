import re
import threading
import time
from datetime import datetime

import requests
from bs4 import BeautifulSoup

import output
from canonicalization import Canonicalizer
from frontier import Frontier, FrontierItem
from robots import Robots


class Crawler:

    def __init__(self):
        self.seed_urls = None
        self.frontier = Frontier()
        self.canonicalizer = Canonicalizer()
        self.all_links = None
        self.crawled_links = set()
        self.count = 0
        self.all_out_links = {}
        self.redirected_map = {}
        self.robots = {}
        self.robots_delay = {}
        self.robots_timer = {}
        self.time_out = 3
        self.total_count = 40000

    def initializer(self, seed_urls):
        self.all_links = set(seed_urls)
        self.seed_urls = seed_urls
        self.frontier.initializer(seed_urls)

    def crawl(self):
        while True:
            if self.frontier.is_empty():
                for url in self.crawled_links:
                    output.write_in_links({url: list(self.frontier.objects[url].in_links)})
                return
            current_wave, score, url = self.frontier.pop()
            domain = self.canonicalizer.get_domain(url)
            if domain not in self.robots:
                try:
                    robots = Robots("http://" + domain + "/robots.txt")
                    self.robots[domain] = robots
                    if robots.delay > self.time_out:
                        self.robots_delay[domain] = self.time_out
                    else:
                        self.robots_delay[domain] = robots.delay
                    self.robots_timer[domain] = datetime.now()
                except Exception:
                    continue
            delay = self.robots_delay[domain]
            if not self.robots[domain].fetch(url):
                continue
            else:
                since_last_crawl = datetime.now() - self.robots_timer[domain]
                if since_last_crawl.total_seconds() < delay:
                    time.sleep(delay - since_last_crawl.total_seconds())
                try:
                    url_head = self.get_header(url)
                    if url_head.status_code == 404:
                        continue
                except Exception:
                    self.robots_timer[domain] = datetime.now()
                    continue
                if "content-type" in url_head.headers:
                    content_type = url_head.headers["content-type"]
                else:
                    content_type = "text/html"
                if "text/html" not in content_type:
                    continue
                else:
                    try:
                        soup, raw_html, base_url, lang = self.get_page(url)
                        self.robots_timer[domain] = datetime.now()
                        if not self.crawl_page(base_url, lang):
                            continue
                        if base_url in self.crawled_links:
                            self.frontier.objects[base_url].in_links.update(self.frontier.objects[url].in_links)
                            continue
                        else:
                            self.crawled_links.add(base_url)
                            frontier_item = FrontierItem(base_url)
                            frontier_item.in_links = self.frontier.objects[url].in_links
                            self.frontier.objects[base_url] = frontier_item
                            self.redirected_map[url] = base_url
                    except Exception:
                        self.robots_timer[domain] = datetime.now()
                        continue
                    raw_out_links = self.get_out_links(soup)
                    out_links = []
                    text = self.get_text(soup)
                    if len(soup.select("title")) != 0:
                        title = soup.select("title")[0].get_text()
                    else:
                        title = None
                    output.write_contents(base_url, text, title)
                    output.write_raw_html({base_url: raw_html})
                    for link in raw_out_links:
                        processed_link = self.canonicalizer.canonicalize(base_url, link)
                        if len(processed_link) != 0:
                            out_links.append(processed_link)
                            if processed_link not in self.all_links:
                                frontier_item = FrontierItem(processed_link, link)
                                frontier_item.update_in_links(base_url)
                                self.frontier.put(frontier_item, current_wave + 1)
                                self.all_links.add(processed_link)
                            else:
                                if processed_link in self.redirected_map:
                                    redirected = self.redirected_map[processed_link]
                                    self.frontier.update_inlinks(redirected, base_url)
                                else:
                                    self.frontier.update_inlinks(processed_link, base_url)
                    output.write_out_links({base_url: out_links})
                self.count += 1
                if self.count == self.total_count:
                    for url in self.crawled_links:
                        output.write_in_links({url: list(self.frontier.objects[url].in_links)})
                    return

    def get_out_links(self, soup):
        a = soup.select('a')
        out_links = []
        for item in a:
            if item.get('href'):
                out_links.append(item['href'])
        return out_links

    def get_page(self, url: str):
        headers = {"Connection": "close"}
        res = requests.get(url=url, headers=headers, timeout=self.time_out)
        soup = BeautifulSoup(res.text, "lxml")
        try:
            if soup.select("html")[0].has_attr("lang"):
                lang = soup.select("html")[0]['lang']
            else:
                lang = "en"
        except Exception:
            lang = "en"
        base_url = res.url
        return soup, res.text, base_url, lang

    def get_header(self, url: str):
        headers = {"Connection": "close"}
        head = requests.head(url=url, headers=headers, timeout=self.time_out, allow_redirects=True)
        return head

    def get_text(self, soup: BeautifulSoup):
        output = ""
        text = soup.find_all("p")
        for t in text:
            new_t = t.get_text()
            new_t = re.sub("\n", "", new_t)
            new_t = re.sub("  +", " ", new_t)
            if len(new_t) == 0:
                continue
            output += "{} ".format(new_t)
        return output

    def crawl_page(self, base_url, lang):
        result = True
        if "en" not in lang.lower():
            result = False
        black_list = [".jpg", ".svg", ".png", ".pdf", ".gif", "youtube", "edit", "footer", "sidebar", "cite",
                      "special", "mailto", ".webm", "tel:", "javascript", ".ogv", "amazon"]
        block = 0
        for key in black_list:
            if key in base_url.lower():
                block = 1
                break
        if block == 1:
            result = False
        return result


seed_urls = ['http://www.nhc.noaa.gov/outreach/history/',
             'https://en.wikipedia.org/wiki/List_of_United_States_hurricanes',
             'http://en.wikipedia.org/wiki/Hurricane_Sandy',
             'https://www.fema.gov/sandy-recovery-office',
             'http://en.wikipedia.org/wiki/Effects_of_Hurricane_Sandy_in_New_York'
             ]

crawler = Crawler()
crawler.initializer(seed_urls)
threading.Thread(target=crawler.crawl())
