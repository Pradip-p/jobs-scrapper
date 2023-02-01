import os
import re
import scrapy
import gc
import uuid
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lazy_crawler.crawler.spiders.base_crawler import LazyBaseCrawler
from lazy_crawler.lib import get_current_date
from lazy_crawler.lib.search import find_numbers

class LazyCrawler(LazyBaseCrawler):

    name = "arbeitnow"

    custom_settings = {
        'DOWNLOAD_DELAY': 6,'AUTOTHROTTLE_ENABLED': True,
        'LOG_LEVEL': 'DEBUG','CHANGE_PROXY_AFTER':1,'USE_PROXY':True,
        'CONCURRENT_REQUESTS' : 1,'CONCURRENT_REQUESTS_PER_IP': 1,'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 2, "COOKIES_ENABLED": True,'DOWNLOAD_TIMEOUT': 180,

        # 'ITEM_PIPELINES' : {
        # 'lazy_crawler.crawler.pipelines.ExcelWriterPipeline': None
        # }
    }

    start_urls = ['https://www.arbeitnow.com/api/job-board-api']

    def parse(self, response):
        res = response.json()
        data = res['data']
        for job in data:
            _id = job['slug']
            company_name = job['company_name']
            title = job['title']
            remote = job['remote']
            location = job['location']
            url = job['url']
            desc = job['description']
            remote = job['remote']
            tags = job['tags']
            job_types = job['job_types']

            data = {
                'created_at': get_current_date(),
                'id':_id,
                'company_name':company_name,
                'job_title':title,
                'country':'Germany',
                'city': location,
                'job_application_url':url,
                'job_description':desc,
                'job_type':','.join(job_types),
                'min_salary':'',
                'max_salary':'',
                'fixed_salay':'',
                'salary_currency':'EUR',
                'division':','.join(tags),
                'company_bio':'',
                'company_logo':'',
                'salary_interval':'yearly',
                'remote_work_policy':str(remote)
            }

            yield scrapy.Request(url, callback=self.get_job, dont_filter=True, meta={'data':data})

        links = res['links'].get('next')
        if links:
            yield scrapy.Request(links, self.parse, dont_filter=True)

    def get_job(self, response):
        data = response.meta.get('data')
        data['job_application_url'] = response.url
        company_url = response.xpath('//a[@itemprop="hiringOrganization"]/@href').extract_first()
        yield scrapy.Request(company_url, callback=self.get_company_desc, dont_filter=True, meta={'data':data})
    
    def get_company_desc(self, response):
        data = response.meta.get('data')
        company_bio = response.xpath('//h3[@itemprop="description"]/text()').extract()

        yield {
            'created_at': get_current_date(),
            'id': data['id'],
            'company_name': data['company_name'],
            'job_title': data['job_title'],
            'country': data['country'],
            'city': '',
            'job_application_url':data['job_application_url'],
            'job_description': data['job_description'],
            'job_type':  data['job_type'],
            'min_salary': '',
            'max_salary': '',
            'fixed_salay': '',
            'salary_currency': 'USD',
            'division':'',
            'company_logo': data['company_logo'],
            'salary_interval': '',
            'remote_work_policy': data['remote_work_policy'],
            'company_bio': ''.join(company_bio),
            }
        gc.collect()
        

settings_file_path = 'lazy_crawler.crawler.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
process = CrawlerProcess(get_project_settings())  
process.crawl(LazyCrawler)
process.start() # the script will block here until the crawling is finished
