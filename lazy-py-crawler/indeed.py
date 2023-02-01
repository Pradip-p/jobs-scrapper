import os
import scrapy
import gc
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lazy_crawler.crawler.spiders.base_crawler import LazyBaseCrawler
import dateparser
import cloudscraper
from bs4 import BeautifulSoup
import js2xml

from lazy_crawler.lib import get_current_date

class LazyCrawler(LazyBaseCrawler):

    name = "indeed"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,'LOG_LEVEL': 'DEBUG','CHANGE_PROXY_AFTER':1,'USE_PROXY':True,
        'CONCURRENT_REQUESTS' : 1,'CONCURRENT_REQUESTS_PER_IP': 1,'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 5, "COOKIES_ENABLED": True,'DOWNLOAD_TIMEOUT': 180,
    
        # 'ITEM_PIPELINES' : {
        # 'lazy_crawler.crawler.pipelines.ExcelWriterPipeline': None
        # }
    }

    url = 'http://example.com'

    allowed_domains = ['www.indeed.com']

    start_urls = [url]

    page_number = 0

    def parse(self, response):
        job_types = ['full+time','part+time','contract','temporary','internship']
        for job in job_types:
            for page in range(0,66):
                url = 'https://www.indeed.com/jobs?q={}&l=United+States&start={}'.format(job,self.page_number)

                self.soup = self.get_soup(url)

                parse_job = self.parse_jobs(self.soup)

                self.page_number += 10

                yield scrapy.Request( self.url, self.parse_job, meta={"parse_job": parse_job}, dont_filter=True)

            

    def parse_job(self, response):
        
        parse_job = response.meta['parse_job']
        for job_dict in parse_job:
            Job_Type = []
            if job_dict['taxonomyAttributes']:
                for job in job_dict['taxonomyAttributes']:
                    if job.get('label')=='job-types':
                        for job_type in  job.get('attributes'):
                            Job_Type.append(job_type.get('label'))
        
            jobkey = str(job_dict.get('jobkey'))
            
            logoUrl = ''

            try:
                logoUrl = job_dict['companyBrandingAttributes'].get('logoUrl')
            except KeyError:
                pass
            
            min_salary = ''

            max_salary = ''

            salary_interval = ''

            try:
                min_salary = job_dict['extractedSalary'].get('min')
                max_salary = job_dict['extractedSalary'].get('max')
                salary_interval = job_dict['extractedSalary'].get('type')
            except KeyError:
                pass

            jobLocationCity = job_dict['jobLocationCity']

            
            frelative_time = job_dict.get('formattedRelativeTime')

            date_posted = dateparser.parse(frelative_time)

            if date_posted == None:
                if 'Just' in frelative_time:
                    Date_Posted = str(
                        dateparser.parse('1 minutes ago').date())
                else:
                    Date_Posted = str(
                        dateparser.parse('30 days ago').date())
            else:

                Date_Posted = str(date_posted.date())

            currency = job_dict['salarySnippet'].get('currency')
            fixed_salary = job_dict['salarySnippet'].get('text')
            job_description = job_dict.get('snippet')

            d_link = 'https://www.indeed.com/rc/clk?jk={jobkey}&atk='.format(
                jobkey=jobkey)

            yield {
                'created_at': get_current_date(),
                'id': jobkey,
                'company_name': job_dict.get('company'),
                'job_title': job_dict.get('title'),
                'country': 'United States',
                'city': jobLocationCity,
                'job_application_url':'https://www.indeed.com/viewjob?jk=' + jobkey,
                'job_description': job_description,
                'job_type': ' | '.join(Job_Type),
                'min_salary': min_salary,
                'max_salary':  max_salary,
                'fixed_salay': fixed_salary,
                'salary_currency': currency,
                'division':'',
                'company_logo': logoUrl,
                'salary_interval': salary_interval,
                'remote_work_policy': 'fully_remote' if jobLocationCity == 'Remote' else '',
                'company_bio': '',
                }
            gc.collect()


    def get_soup(self,url):
        
        scraper = cloudscraper.create_scraper(delay=10)
        # scraper = cloudscraper.create_scraper(disableCloudflareV1=True)
        response = scraper.get(url)

        soup = BeautifulSoup(response.text, 'lxml')

        return soup


    def parse_jobs(self, soup):
        script = soup.find('script', {'id': 'mosaic-data'}).text
        parsed = js2xml.parse(script)
        results = js2xml.jsonlike.make_dict(parsed.xpath('//property[@name="results"]/array')[0])
        return results

    

settings_file_path = 'lazy_crawler.crawler.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
process = CrawlerProcess(get_project_settings())  
process.crawl(LazyCrawler)
process.start() # the script will block here until the crawling is finished

