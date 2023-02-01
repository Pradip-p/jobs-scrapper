import os
import scrapy
import gc
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lazy_crawler.crawler.spiders.base_crawler import LazyBaseCrawler
from lazy_crawler.lib import get_current_date


class LazyCrawler(LazyBaseCrawler):

    name = "shopify"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,'LOG_LEVEL': 'DEBUG','CHANGE_PROXY_AFTER':1,'USE_PROXY':True,
        'CONCURRENT_REQUESTS' : 1,'CONCURRENT_REQUESTS_PER_IP': 1,'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 2, "COOKIES_ENABLED": True,'DOWNLOAD_TIMEOUT': 180,

        # 'ITEM_PIPELINES' : {
        # 'lazy_crawler.crawler.pipelines.ExcelWriterPipeline': None
        # }
    }

    start_urls = ['https://www.shopify.com/careers']


    def parse(self, response):
        urls = response.xpath('//a[@class="job-card__container"]/@href').extract()
        for url in urls:
            url = 'https://www.shopify.com{}'.format(url)
            yield scrapy.Request(url, callback=self.parse_job_details,dont_filter=True)

        next_page = response.xpath('//a[@class="pagination-button pagination-next link link--secondary"]/@href').extract_first()
        if next_page:
            url = 'https://www.shopify.com{}'.format(next_page)
            yield scrapy.Request(url, callback=self.parse, dont_filter=True)

    def parse_job_details(self, response):
        
        JobTitle = response.xpath('//h1[@class="heading heading--1"]/text()').extract_first()
        company_bio = response.xpath('//div[@class="long-form-content "]/p/text()').extract()
        posting__section = response.xpath('//li[@class="job-posting__section-summary-item"]/text()').extract()
        country = posting__section[0]
        division = posting__section[-1]
        job_application_url = response.url
        roleDescription = ''.join(response.xpath('//div[@class="job-posting__section-content markdown-lists"]/p//text()').extract())

        yield {
        'created_at': get_current_date(),
        'id': '',
        'company_name': 'Shopify',
        'job_title': JobTitle,
        'country': country,
        'city': '',
        'job_application_url':job_application_url,
        'job_description': roleDescription,
        'job_type': '',
        'min_salary': '',
        'max_salary': '',
        'fixed_salay': '',
        'salary_currency': '',
        'division':division,
        'company_logo': '',
        'salary_interval': '',
        'remote_work_policy': '',
        'company_bio': ''.join(company_bio),
        }
    gc.collect()
        



settings_file_path = 'lazy_crawler.crawler.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
process = CrawlerProcess(get_project_settings())  
process.crawl(LazyCrawler)
process.start() # the script will block here until the crawling is finished

