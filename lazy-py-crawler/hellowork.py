import os
import gc
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lazy_crawler.crawler.spiders.base_crawler import LazyBaseCrawler
from lazy_crawler.lib import get_current_date

class LazyCrawler(LazyBaseCrawler):

    name = "snaphunt"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,'LOG_LEVEL': 'DEBUG','CHANGE_PROXY_AFTER':1,'USE_PROXY':True,
        'CONCURRENT_REQUESTS' : 1,'CONCURRENT_REQUESTS_PER_IP': 1,'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 10, "COOKIES_ENABLED": True,'DOWNLOAD_TIMEOUT': 180,

        # 'ITEM_PIPELINES' : {
        # 'lazy_crawler.crawler.pipelines.ExcelWriterPipeline': None
        # }
    }
    page_numer = 1

    start_urls = ['https://www.hellowork.com/searchoffers/getsearchfacets?p={}'.format(page_numer)]


    def parse(self, response):
        
        res = response.json()
        job_lists = res['Results']

        for job in job_lists:
            CompanyName = job['CompanyName']
            CompanyLogo = job['CompanyLogo']
            _id = job['Id']
            url = 'https://www.hellowork.com{}'.format(job['UrlOffre'])

            title = job['OfferTitle']
            candidateDescription = job['Profile']
            roleDescription = job.get('Description')
            city = job['Localisation']
            country = 'France'
            division = job.get('Domaine')
            SalaryDisplay = job.get('SalaryDisplay')
            salary_interval = SalaryDisplay.split()
            if salary_interval:
                salary_interval = salary_interval[-1]

            else:
                salary_interval = ''

            s = SalaryDisplay.split('par')

            if s:
                min_salary = s[0].split('-')[0]
                max_salary = s[0].split('-')[-1].replace('EUR','')
            salary_currency = 'EUR'
            job_types = job['ContractType']

            PublishDate = job['PublishDate']
            
            yield {
            'created_at': get_current_date(),
            'id': _id,
            'company_name': CompanyName,
            'job_title': title,
            'country': country,
            'city': city,
            'job_application_url':url,
            'job_description': roleDescription,
            'job_type': job_types,
            'min_salary': min_salary,
            'max_salary': max_salary,
            'fixed_salay': SalaryDisplay,
            'salary_currency': salary_currency,
            'division':division,
            'company_logo': CompanyLogo,
            'salary_interval': salary_interval,
            'remote_work_policy': '',
            'company_bio': '',
            }
            
        gc.collect()

        totalHits= res['TotalHits']
        per_page_sample = res['PageInfos'].get('PageSize')
        total_page = int(totalHits) // int(per_page_sample)
        self.page_numer +=1

        if self.page_numer <= total_page:
            url = 'https://www.hellowork.com/searchoffers/getsearchfacets?p={}'.format(self.page_numer)
            yield scrapy.Request(url, self.parse, dont_filter=True)


settings_file_path = 'lazy_crawler.crawler.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
process = CrawlerProcess(get_project_settings())  
process.crawl(LazyCrawler)
process.start() # the script will block here until the crawling is finished

