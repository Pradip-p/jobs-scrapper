import gc
import os
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lazy_crawler.crawler.spiders.base_crawler import LazyBaseCrawler
import json
from lazy_crawler.lib import get_current_date
from lazy_crawler.lib.search import find_numbers, get_hashtags
class LazyCrawler(LazyBaseCrawler):

    name = "docker"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,'LOG_LEVEL': 'DEBUG','CHANGE_PROXY_AFTER':1,'USE_PROXY':True,
        'CONCURRENT_REQUESTS' : 1,'CONCURRENT_REQUESTS_PER_IP': 1,'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 10, "COOKIES_ENABLED": True,'DOWNLOAD_TIMEOUT': 180,

        # 'ITEM_PIPELINES' : {
        # 'lazy_crawler.crawler.pipelines.ExcelWriterPipeline': None
        # }
    }

    allowed_domains = ['jobs.ashbyhq.com']

    headers = {"Content-Type": "application/json"}


    def start_requests(self):
        url = 'https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams'
        
        data = {"operationName":"ApiJobBoardWithTeams","variables":{"organizationHostedJobsPageName":"docker"},"query":"query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {\n  jobBoard: jobBoardWithTeams(\n    organizationHostedJobsPageName: $organizationHostedJobsPageName\n  ) {\n    teams {\n      id\n      name\n      parentTeamId\n      __typename\n    }\n    jobPostings {\n      id\n      title\n      teamId\n      locationId\n      locationName\n      employmentType\n      secondaryLocations {\n        ...JobPostingSecondaryLocationParts\n        __typename\n      }\n      compensationTierSummary\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment JobPostingSecondaryLocationParts on JobPostingSecondaryLocation {\n  locationId\n  locationName\n  __typename\n}"}
        yield scrapy.Request(url, method="POST", headers=self.headers,body=json.dumps(data))
        


    def parse(self, response):
        
        res = response.json()
        jobPostings = res['data']['jobBoard'].get('jobPostings')

        for job in jobPostings:
            _id = job['id']

            url = 'https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobPosting'

            data = {"operationName":"ApiJobPosting","variables":{"organizationHostedJobsPageName":"docker","jobPostingId": _id},"query":"query ApiJobPosting($organizationHostedJobsPageName: String!, $jobPostingId: String!) {\n  jobPosting(\n    organizationHostedJobsPageName: $organizationHostedJobsPageName\n    jobPostingId: $jobPostingId\n  ) {\n    id\n    title\n    departmentName\n    locationName\n    employmentType\n    descriptionHtml\n    isListed\n    isConfidential\n    teamNames\n    applicationForm {\n      ...FormRenderParts\n      __typename\n    }\n    surveyForms {\n      ...FormRenderParts\n      __typename\n    }\n    secondaryLocationNames\n    compensationTierSummary\n    compensationTiers {\n      id\n      title\n      tierSummary\n      __typename\n    }\n    compensationTierGuideUrl\n    scrapeableCompensationSalarySummary\n    compensationPhilosophyHtml\n    __typename\n  }\n}\n\nfragment FormRenderParts on FormRender {\n  id\n  formControls {\n    identifier\n    title\n    __typename\n  }\n  errorMessages\n  sections {\n    title\n    descriptionHtml\n    fieldEntries {\n      ...FormFieldEntryParts\n      __typename\n    }\n    __typename\n  }\n  sourceFormDefinitionId\n  __typename\n}\n\nfragment FormFieldEntryParts on FormFieldEntry {\n  id\n  field\n  fieldValue {\n    ... on JSONBox {\n      ...JSONBoxParts\n      __typename\n    }\n    ... on File {\n      ...FileParts\n      __typename\n    }\n    ... on FileList {\n      files {\n        ...FileParts\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  isRequired\n  descriptionHtml\n  __typename\n}\n\nfragment JSONBoxParts on JSONBox {\n  value\n  __typename\n}\n\nfragment FileParts on File {\n  id\n  url\n  filename\n  __typename\n}"}
            yield scrapy.Request(url, method="POST", callback=self.job_details, headers=self.headers,body=json.dumps(data), meta= {'job_title' : job['title'],
            'city' : job['locationName'],
            'job_type' : job['employmentType']
            })

    def job_details(self, response):
        hashtags = ''
        res = response.json()
        jobPosting = res['data'].get('jobPosting')
        
        job_title = jobPosting['title']
        division = jobPosting['departmentName']
        country = []
        country.append(jobPosting['locationName'])
        secondaryLocationNames = list(jobPosting['secondaryLocationNames'])
        if secondaryLocationNames:
            country.extend(secondaryLocationNames)

        employmentType = jobPosting['employmentType']
        
        job_description = jobPosting['descriptionHtml'] 
        #get the hash tag value from job descriptions
        hashtags = get_hashtags(job_description)
        remote_work_policy = ''
        if hashtags:
            remote_work_policy = 'fully_remote'

        number = find_numbers(job_description)

        teamNames = ','.join(jobPosting['teamNames'])

        _id = jobPosting['id']

        job_application_url = 'https://jobs.ashbyhq.com/docker/{}'.format(_id)

        company_name = 'docker'

        # salaryCurrency = job['normalizedJobPosting'].get('salaryCurrency')

        yield {
            'created_at': get_current_date(),
            'id': _id,
            'company_name': company_name,
            'job_title': job_title,
            'country':','.join(country),
            'city': '',
            'job_application_url':job_application_url,
            'job_description': job_description,
            'job_type':  response.meta['job_type'],
            'min_salary': '',
            'max_salary': '',
            'fixed_salay': '',
            'salary_currency': 'USD',
            'division':division,
            'company_logo': '',
            'salary_interval': '',
            'remote_work_policy': remote_work_policy,
            'company_bio': '',
        }
        gc.collect()

    
settings_file_path = 'lazy_crawler.crawler.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
process = CrawlerProcess(get_project_settings())  
process.crawl(LazyCrawler)
process.start() # the script will block here until the crawling is finished
