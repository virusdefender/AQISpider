# coding=utf-8
import re
import sqlite3
import logging
import datetime

import requests

logging.basicConfig(level=logging.DEBUG)


class BaseDBHandler(object):
    def __init__(self, db_path):
        self.db_path = db_path

    def execute_sql(self, *args):
        conn, cursor = self.db_connection
        if len(args) == 1:
            # 只执行 sql 语句的
            cursor.execute(args[0])
        else:
            # insert 或者 update，后面还绑定数据的
            cursor.execute(args[0], args[1])
        conn.commit()
        return cursor

    @property
    def db_connection(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        return conn, cursor


class DBHandler(BaseDBHandler):
    def __init__(self, db_path="result.db"):
        self.db_path = db_path
        super(DBHandler, self).__init__(self.db_path)
        self.init_db()

    def init_db(self):
        self.execute_sql("CREATE TABLE IF NOT EXISTS "
                         "result "
                         "(id INTEGER PRIMARY KEY, "
                         "city VARCHAR(20), "
                         "web_id INTEGER , "
                         "date DATE, "
                         "AQI INTEGER, "
                         "text VARCHAR(20), "
                         "major_pollutant VARCHAR (20))")


class Spider(object):
    def __init__(self):
        self.base_url = "http://datacenter.mep.gov.cn/report/air_daily/air_dairy.jsp?city=%E5%8C%97%E4%BA%AC%E5%B8%82&startdate=2015-01-01&enddate=2015-12-31&page={page}"
        self.regex = re.compile(r'<td.*class="report1_[3|5]".*>(.*)</td>')
        self.db = DBHandler()

    def request(self, url):
        user_agent = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                     "Chrome/44.0.2403.130 Safari/537.36"
        headers = {'User-Agent': user_agent}
        # 重试三次，还不行就抛异常
        for i in range(0, 3):
            try:
                response = requests.get(url, headers=headers)
                # 防止403等
                if response.status_code != 200:
                    raise ValueError()
                return response.content
            except Exception as e:
                if i == 2:
                    raise e
                continue

    def craw(self):
        for page in range(1, 14):
            url = self.base_url.format(page=str(page))
            html = self.request(url)
            # logging.debug(html)
            logging.debug("Crawling page {page}\n".format(page=page))
            page_data = self.get_page_data(html)
            self.save_to_db(page_data)

    def get_page_data(self, html):
        # 6个分组，然后组合
        l = self.regex.findall(html)
        length = len(l)
        if length % 6 != 0:
            l = l[1::]
            length = len(l)
        ret = []
        for i in range(0, length / 6):
            day_data_list = l[i * 6: i * 6 + 6]
            day_data = {"web_id": day_data_list[0], "city": day_data_list[1],
                        "date": day_data_list[2], "AQI": day_data_list[3],
                        "text": day_data_list[4], "major_pollutant": day_data_list[5]}
            logging.debug(day_data)
            ret.append(day_data)
        return ret

    def save_to_db(self, page_data):
        for day_data in page_data:
            self.db.execute_sql("INSERT INTO result(web_id, city, date, AQI, text, major_pollutant) "
                                "VALUES(?, ?, ?, ?, ?, ?)",
                                (day_data["web_id"], day_data["city"].decode("utf-8"),
                                 datetime.datetime.strptime(day_data["date"], "%Y-%m-%d"),
                                 int(day_data["AQI"] or -1), day_data["text"].decode("utf-8"), day_data["major_pollutant"].decode("utf-8")))


s = Spider()
s.craw()
