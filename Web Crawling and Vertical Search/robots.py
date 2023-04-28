import urllib.request
import urllib.robotparser


class RobotTimeout(urllib.robotparser.RobotFileParser):
    def __init__(self, url='', timeout=3):
        super().__init__(url)
        self.timeout = timeout

    def read(self):
        self.disallow_all = False
        self.allow_all = False
        try:
            f = urllib.request.urlopen(self.url, timeout=self.timeout)
        except urllib.error.HTTPError as err:
            if err.code in (401, 403):
                self.disallow_all = True
            elif err.code >= 400:
                self.allow_all = True
        else:
            raw = f.read()
            self.parse(raw.decode("utf-8").splitlines())


class Robots:

    def __init__(self, url):
        self.url = url
        self.r = self.initializer()
        self.delay = 1.0
        self.get_delay()

    def initializer(self):
        r = RobotTimeout()
        r.set_url(self.url)
        r.read()
        return r

    def get_delay(self):
        delay = self.r.crawl_delay(useragent="*")
        if delay is not None:
            self.delay = delay

    def fetch(self, new_url):
        return self.r.can_fetch("*", new_url)
