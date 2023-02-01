import scrapy
class LazyBaseCrawler(scrapy.Spider):

    name = "lazy_base_crawler"

    allowed_domains = [""]

    # START URLS for your project.
    start_urls = ['']
