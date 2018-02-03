import scrapy
from amazonscrap.items import AmazonscrapItem
from scrapy import signals

# to run : scrapy crawl book-scraper
class BooksSpider(scrapy.Spider):
    name = "book-scraper"
    start_urls = [
        'https://www.amazon.com/Python-Crash-Course-Hands-Project-Based/product-reviews/1593276036/',
        'https://www.amazon.com/Python-Algorithmic-Thinking-Complete-Beginner-ebook/product-reviews/B01C62O7DK/',
        'https://www.amazon.com/Python-Algorithmic-Thinking-Complete-Beginner-ebook/product-reviews/B01C62O7DK/',
        'https://www.amazon.com/Python-Tweens-Teens-Computational-Algorithmic-ebook/product-reviews/B07769VP7Q/',
        'https://www.amazon.com/Python-Programming-Intelligence-Reinforcement-Convolutional/product-reviews/1978317115/',
        'https://www.amazon.com/Python-Scientists-John-M-Stewart/product-reviews/1316641236/',
        'https://www.amazon.com/Web-Scraping-Python-Collecting-Modern/product-reviews/1491910291/',
        'https://www.amazon.com/Illustrated-Guide-Python-Walkthrough-Illustrations/product-reviews/1977921752/',
    ]

    books_already_scrapped = list()
    base_url = "https://www.amazon.com/Python-Crash-Course-Hands-Project-Based/product-reviews/"

    def parse(self, response):
        book = AmazonscrapItem()
        book["title"] = response.css('a[data-hook="product-link"]::text').extract_first()
        book["title"] = self.escape(book["title"])
        rating = response.css('i[data-hook="average-star-rating"]')
        rating = rating.css('span[class="a-icon-alt"]::text').extract_first()
        book["rating"] = float(rating.replace(" out of 5 stars",""))
        review_count = response.css('span[data-hook="total-review-count"]::text').extract_first()
        review_count = review_count.replace(",","")
        book["review_count"] = int(review_count)
        book["author"] = response.css('a[class="a-size-base a-link-normal"]::text').extract_first()
        book["book_id"] = self.get_book_id(response)
        reviews = self.get_reviews(response)
        book["reviews"] = reviews

        if self.is_python_book(book):
            yield book

        self.books_already_scrapped.append(book["book_id"])

        more_book_ids = self.get_more_books(response)
        for book_id in more_book_ids:
            if book_id not in self.books_already_scrapped:
                next_url = self.base_url + book_id + "/"
                yield scrapy.Request(next_url, callback=self.parse)

    @staticmethod
    def get_book_id(response):
        url = response.request.url
        url = url.split("/")
        return url[-2]

    def get_reviews(self, response):
        reviews = list()
        reviews_list_tag = response.css('div[id="cm_cr-review_list"]')
        reviews_list = reviews_list_tag.css('div[data-hook="review"]')
        for review in reviews_list:
            review_data = dict()
            rating = review.css('span[class="a-icon-alt"]::text').extract_first()
            review_data["rating"] = float(rating.replace(" out of 5 stars",""))
            review_data["subject"] = review.css('a[data-hook="review-title"]::text').extract_first()
            review_data["subject"] = self.escape(review_data["subject"])
            review_data["author"] = review.css('a[data-hook="review-author"]::text').extract_first()
            review_date = review.css('span[data-hook="review-date"]::text').extract_first()
            review_data["review_date"] = review_date.replace("on ","")
            review_body = review.css('span[data-hook="review-body"]::text').extract() # with html tags in it
            review_body = self.concate_review(review_body)
            review_data["review_body"] = self.escape(review_body)
            review_data["review_id"] = review.css('div[data-hook="review"]::attr(id)').extract_first() # with html tags in it

            reviews.append(review_data)

        return reviews

    @staticmethod
    def get_more_books(response):
        book_ids = set()
        more_books = response.css('div[class="a-fixed-left-grid a-spacing-medium"]')
        for book in more_books:
            book_link = book.css('a[data-hook="product-link"]::attr(href)').extract_first()
            book_id = book_link.split("/")[-2]
            book_ids.add(book_id)

        return book_ids

    def spider_opened(self):
        print("\n\n\nSpider Opened\n\n\n")
        import MySQLdb

        conn = MySQLdb.connect(host="localhost",  # your host
                               user="root",  # username
                               passwd="root",  # password
                               db="anuragrana_db")  # name of the database

        # Create a Cursor object to execute queries.
        cur = conn.cursor()
        cur.execute("SELECT book_id FROM books order by sys_id")
        book_ids = list()
        for row in cur.fetchall():
            book_ids.append(row[0])

        print(book_ids)
        self.books_already_scrapped = book_ids
        if book_ids:
            url =  self.start_urls[0]
            url = url.split("/")
            url[-2] = book_ids[-1]
            self.start_urls.append("/".join(x for x in url))


        print("\n\nStarting with")
        print(self.start_urls)
        cur.close()
        conn.close()


    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BooksSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_opened, signal=signals.spider_opened)
        return spider

    @staticmethod
    def escape(text):
        text = text.replace("'","''")
        return text

    @staticmethod
    def concate_review(review):
        return "<br>".join(x for x in review)

    @staticmethod
    def is_python_book(item):
        keywords = ("python", "django", "flask",)
        if any(x in item["title"].lower() for x in keywords):
            return True
        review_subjects = [x["subject"] for x in item["reviews"]]
        if any(x in review_subjects for x in keywords):
            return True
        review_comments = [x["review_body"] for x in item["reviews"]]
        if any(x in review_comments for x in keywords):
            return True

        return False





