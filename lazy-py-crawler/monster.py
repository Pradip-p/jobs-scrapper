import os
import scrapy
import gc
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lazy_crawler.crawler.spiders.base_crawler import LazyBaseCrawler
import json
from lazy_crawler.lib import get_current_date

class LazyCrawler(LazyBaseCrawler):

    name = "monster"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,'LOG_LEVEL': 'DEBUG','CHANGE_PROXY_AFTER':1,'USE_PROXY':True,
        'CONCURRENT_REQUESTS' : 1,'CONCURRENT_REQUESTS_PER_IP': 1,'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 10, "COOKIES_ENABLED": True,'DOWNLOAD_TIMEOUT': 180,

        # 'ITEM_PIPELINES' : {
        # 'lazy_crawler.crawler.pipelines.ExcelWriterPipeline': None
        # }
    }

    allowed_domains = ['api.monster.io','www.monster.com','appsapi.monster.io']

    headers = {"Content-Type": "application/json"}

    offset =  0 

    def start_requests(self):
        url = 'https://appsapi.monster.io/jobs-svx-service/v2/monster/search-jobs/samsearch/en-US?apikey=AE50QWejwK4J73X1y1uNqpWRr2PmKB3S'
        headers = self.headers
    
        data= {"jobQuery":{"query":"","locations":[{"country":"us","address":"united states","radius":{"unit":"mi","value":100}}]},"jobAdsRequest":{"position":[1,2,3,4,5,6,7,8,9],"placement":{"channel":"WEB","location":"JobSearchPage","property":"monster.com","type":"JOB_SEARCH","view":"SPLIT"}},"fingerprintId":"2fa44cf6519c13e3f11377831218e1fe","offset":self.offset,"pageSize":9,"histogramQueries":["count(company_display_name)","count(employment_type)"],"includeJobs":[]}
        yield scrapy.Request(url, method="POST", headers=headers,body=json.dumps(data))
        


    def parse(self, response):
        
        res = response.json()
        jobResults = res.get('jobResults')

        for job in jobResults:
            _id = job['jobId']
            job_description = job['jobPosting'].get('description')
            job_application_url = job['jobPosting'].get('url')
            job_datePosted = job['jobPosting'].get('datePosted')
            company_name = job['jobPosting']['hiringOrganization'].get('name')
            job_title = job['jobPosting'].get('title')
            jobLocation = job['jobPosting']['jobLocation']
            for loc in jobLocation:
                addressCountry = loc.get('address').get('addressCountry')
                city = '{},{}'.format(loc.get('address').get('addressLocality'),loc.get('address').get('addressRegion'))
            applyUrl = job['apply'].get('applyUrl')
            salaryCurrency = job['normalizedJobPosting'].get('salaryCurrency')
            division = job['normalizedJobPosting'].get('occupationalCategory')

            yield {
                'created_at': get_current_date(),
                'id': _id,
                'company_name': company_name,
                'job_title': job_title,
                'country': addressCountry,
                'city': city,
                'job_application_url':applyUrl,
                'job_description': job_description,
                'job_type': '',
                'min_salary': '',
                'max_salary': '',
                'fixed_salay': '',
                'salary_currency': salaryCurrency,
                'division':division,
                'company_logo': '',
                'salary_interval': '',
                'remote_work_policy': '',
                'company_bio': '',
            }
        gc.collect()
        self.offset += 9

        url = 'https://appsapi.monster.io/jobs-svx-service/v2/monster/search-jobs/samsearch/en-US?apikey=AE50QWejwK4J73X1y1uNqpWRr2PmKB3S'
        data = {"jobQuery":{"query":"","locations":[{"country":"us","address":"united states","radius":{"unit":"mi","value":100}}]},"jobAdsRequest":{"position":[1,2,3,4,5,6,7,8,9],"placement":{"channel":"WEB","location":"JobSearchPage","property":"monster.com","type":"JOB_SEARCH","view":"SPLIT"}},"fingerprintId":"2fa44cf6519c13e3f11377831218e1fe","offset":self.offset,"pageSize":9,"histogramQueries":["count(company_display_name)","count(employment_type)"]}
        yield response.follow(url, callback=self.parse,  method="POST", headers=self.headers,body=json.dumps(data))

    
settings_file_path = 'lazy_crawler.crawler.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
process = CrawlerProcess(get_project_settings())  
process.crawl(LazyCrawler)
process.start() # the script will block here until the crawling is finished



# {"jobQuery":{"query":"","locations":[{"country":"us","address":"united states","radius":{"unit":"mi","value":20}}]},"jobAdsRequest":{"position":[1,2,3,4,5,6,7,8,9],"placement":{"channel":"WEB","location":"JobSearchPage","property":"monster.com","type":"JOB_SEARCH","view":"SPLIT"}},"fingerprintId":"2fa44cf6519c13e3f11377831218e1fe","offset":0,"pageSize":9,"histogramQueries":["count(company_display_name)","count(employment_type)"],"includeJobs":[]}
# {"jobQuery":{"query":"","locations":[{"country":"us","address":"united states","radius":{"unit":"mi","value":20}}]},"jobAdsRequest":{"position":[1,2,3,4,5,6,7,8,9],"placement":{"channel":"WEB","location":"JobSearchPage","property":"monster.com","type":"JOB_SEARCH","view":"SPLIT"}},"fingerprintId":"2fa44cf6519c13e3f11377831218e1fe","offset":9,"pageSize":9,"histogramQueries":["count(company_display_name)","count(employment_type)"],"searchId":"627a74e9-6cb4-4fb8-bc86-ed37891079e2"}