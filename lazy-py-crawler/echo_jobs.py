import os
import scrapy
import gc
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lazy_crawler.crawler.spiders.base_crawler import LazyBaseCrawler
from lazy_crawler.lib import get_current_date


class LazyCrawler(LazyBaseCrawler):
    page_number = 1

    name = "echo_jobs"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,'LOG_LEVEL': 'DEBUG','CHANGE_PROXY_AFTER':1,'USE_PROXY':True,
        'CONCURRENT_REQUESTS' : 1,'CONCURRENT_REQUESTS_PER_IP': 1,'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 2, "COOKIES_ENABLED": True,'DOWNLOAD_TIMEOUT': 180,

        # 'ITEM_PIPELINES' : {
        # 'lazy_crawler.crawler.pipelines.ExcelWriterPipeline': None
        # }
    }

    start_urls = ['https://api.echojobs.io/v1/search?page={}&limit=35&sort_by=rank'.format(page_number)]


    def parse(self, response):
        
        res = response.json()
        data = res['result']
        for job in data:
            _id = ''
            remote_work_policy = ''
            city = ''
            company_name = job['company'].get('profile_name')
            logo = job['company'].get('logo')
            title = job['title']
            location = job['locations'] #[Tempe, AZ, USA,Remote]
            url = job['url']
            tags = job['skills']
            if tags:
                tags = ','.join(tags)
            else:
                tags = ''
                
            job_types = job['seniority']
            min_salary = job['min_pay']
            max_salary = job['max_pay']
            
            if location:
                for key, val in enumerate(location):
                    if val == 'Remote':
                        remote_work_policy = val
                
                try:
                    city = ''.join([x for x in location if x != 'US' if x != 'Canada' if x!= 'Remote' if x != 'USA'])
                except IndexError:
                    city = ''
            

            data = {
                'id': _id,
                'company_name':company_name,
                'job_title':title,
                'country':'US',
                'city': city,
                'job_application_url':url,
                'job_type':''.join(job_types),
                'min_salary': min_salary,
                'max_salary':max_salary,
                'fixed_salay':'',
                'salary_currency':'USD',
                'division': tags,
                'company_bio':'',
                'company_logo':logo,
                'salary_interval':'yearly',
                'remote_work_policy': ''.join(remote_work_policy)
            }

            url = 'https://api.echojobs.io/v1/job/{}'.format(job['job_handle'])
            yield scrapy.Request(url, callback=self.get_content, dont_filter=True, meta={'data':data})
        
        total_pages = res['pages']
        if self.page_number <= total_pages:
            self.page_number += 1
            url = 'https://api.echojobs.io/v1/search?page={}&limit=35&sort_by=rank'.format(self.page_number)
            yield scrapy.Request(url, self.parse, dont_filter=True)

    def get_content(self, response):
        data = response.meta.get('data')

        res = response.json()
        content = res['content']

        yield {
        'created_at': get_current_date(),
        'id':'',
        'company_name': data.get('company_name'),
        'job_title': data.get('job_title'),
        'country': data.get('country'),
        'city': data.get('city'),
        'job_application_url': data.get('job_application_url'),
        'job_description': content,
        'job_type': data.get('job_type'),
        'min_salary': data.get('min_salary'),
        'max_salary': data.get('max_salary'),
        'fixed_salay': data.get('fixed_salay'),
        'salary_currency': data.get('salary_currency'),
        'division': data.get('division'),
        'company_logo': data.get('salary_interval'),
        'salary_interval': data.get('salary_interval'),
        'remote_work_policy': data.get('remote_work_policy'),
        'company_bio': data.get('company_bio'),
        }
    gc.collect()

settings_file_path = 'lazy_crawler.crawler.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
process = CrawlerProcess(get_project_settings())  
process.crawl(LazyCrawler)
process.start() # the script will block here until the crawling is finished

