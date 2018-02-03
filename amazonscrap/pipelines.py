# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
from datetime import datetime


class AmazonscrapPipeline(object):

    conn = None
    cur = None

    def process_item(self, item, spider):
        # save book
        sql = "insert into books (book_id, title, author, rating, review_count) " \
              "VALUES ('%s', '%s', '%s', '%s', '%s' )" % \
              (item["book_id"], item["title"], item["author"], item["rating"], item["review_count"])
        try:
            self.cur.execute(sql)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(repr(e))

        # saving reviews
        reviews = item["reviews"]

        for review in reviews:
            review_date = review["review_date"]
            review_date = self.convert_date(review_date)
            sql = "insert into reviews (review_id, book_id_id, subject, author, rating, review_text, review_date) " \
                  "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s' )" % \
                  (review["review_id"], item["book_id"], review["subject"], review["author"], review["rating"],
                   review["review_body"], review_date)
            try:
                self.cur.execute(sql)
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(repr(e))
                print(item["book_id"])

    def open_spider(self, spider):
        self.conn = MySQLdb.connect(host="localhost",  # your host
                             user="root",  # username
                             passwd="root",  # password
                             db="anuragrana_db")  # name of the database

        # Create a Cursor object to execute queries.
        self.cur = self.conn.cursor()

    def close_spider(self, spider):
        self.cur.close()
        self.conn.close()

    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    @staticmethod
    def convert_date(date_string):
        date_object = datetime.strptime(date_string, '%B %d, %Y')
        return date_object



