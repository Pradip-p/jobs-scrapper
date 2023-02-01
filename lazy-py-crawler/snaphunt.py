import os
import scrapy
import gc
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lazy_crawler.crawler.spiders.base_crawler import LazyBaseCrawler
from lazy_crawler.lib import get_current_date


class LazyCrawler(LazyBaseCrawler):

    name = "snaphunt"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,'LOG_LEVEL': 'DEBUG','CHANGE_PROXY_AFTER':1,'USE_PROXY':True,
        'CONCURRENT_REQUESTS' : 1,'CONCURRENT_REQUESTS_PER_IP': 1,'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 2, "COOKIES_ENABLED": True,'DOWNLOAD_TIMEOUT': 180,

        # 'ITEM_PIPELINES' : {
        # 'lazy_crawler.crawler.pipelines.ExcelWriterPipeline': None
        # }
    }

    start_urls = ['https://api.snaphunt.com/v2/jobs?pageSize=15&isFeatured=false']


    def parse(self, response):
        
        res = response.json()
        body = res.get('body')
        job_lists = body.get('list')

        for job in job_lists:
            _id = job['_id']
            jobReferenceId = job['jobReferenceId']
            url = 'https://snaphunt.com/jobs/{}'.format(jobReferenceId)

            remote_work_policy = ''
            try:
                remote_work = job['remoteLocation']

                if remote_work:
                    remote_work_policy = 'True'
            except KeyError:
                pass

            

            title = job['jobTitle']
            locations = job['location'] 
            country = ''
            city = ''
            for location  in locations:
                country = location.get('country')
                city = location.get('city')

            jobListing = job.get('jobListing')
            roleDescription = jobListing.get('roleDescription')
            candidateDescription = jobListing.get('candidateDescription')

                
            min_salary = job['minSalary']
            max_salary = job['maxSalary']
            salary_currency = job['currency']
            job_types = job['jobEngagement']
            
            yield {
            'created_at': get_current_date(),
            'id': _id,
            'company_name': '',
            'job_title': title,
            'country': country,
            'city': city,
            'job_application_url':url,
            'job_description': roleDescription,
            'job_type': job_types,
            'min_salary': min_salary,
            'max_salary': max_salary,
            'fixed_salay': '',
            'salary_currency': salary_currency,
            'division':candidateDescription,
            'company_logo': '',
            'salary_interval': 'yearly',
            'remote_work_policy': remote_work_policy,
            'company_bio': '',
            }
        gc.collect()

        meta= body.get('meta')
        if meta.get('hasNext'):
            url = 'https://api.snaphunt.com/v2/jobs?next={}&pageSize=15&isFeatured=false'.format(meta.get('next'))
            yield scrapy.Request(url, self.parse, dont_filter=True)


settings_file_path = 'lazy_crawler.crawler.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
process = CrawlerProcess(get_project_settings())  
process.crawl(LazyCrawler)
process.start() # the script will block here until the crawling is finished

