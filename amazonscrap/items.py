# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class AmazonscrapItem(scrapy.Item):
    book_id = scrapy.Field()
    title = scrapy.Field()
    author = scrapy.Field()
    rating = scrapy.Field()
    review_count = scrapy.Field()
    reviews = scrapy.Field()
