import os
import scrapy
import gc
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from lazy_crawler.crawler.spiders.base_crawler import LazyBaseCrawler
import json
from lazy_crawler.lib.date import get_current_date

class LazyCrawler(LazyBaseCrawler):

    name = "angel"

    custom_settings = {
        'DOWNLOAD_DELAY': 2,'LOG_LEVEL': 'DEBUG',
        'CONCURRENT_REQUESTS' : 1,'CONCURRENT_REQUESTS_PER_IP': 1,'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'RETRY_TIMES': 20, "COOKIES_ENABLED": True,'DOWNLOAD_TIMEOUT': 180,  'RANDOMIZE_DOWNLOAD_DELAY': True,
        'ITEM_PIPELINES' : {
        'lazy_crawler.crawler.pipelines.ExcelWriterPipeline': None
        }
    }

    allowed_domains = ['angel.co']

    headers = {
        'Host':'angel.co',
        'User-Agent':' Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'content-type': 'application/json',
        'x-requested-with': 'XMLHttpRequest',
        'apollographql-client-name': 'talent-web',
        'X-AL-GQL': "LxjnZ7woxyxOkotP4BZseF1HqMyZGAK71SGkUSKPr%2FI%3D",
        'Connection': 'keep-alive',
        'Origin': 'https://angel.co',
        'x-angellist-dd-client-referrer-resource': '/role/:role',
    }

    page_number = 1

    locations = ['united-states','india','germany','france','mexico','italy','israel','spain','portugal','belgium','united-kingdom','australia']
    def start_requests(self):        
        for location in self.locations:
            url = 'https://angel.co/graphql?fallbackAOR=talent'
            #data for role define, with python develper
            # data = {"operationName":"SeoLandingRoleSearchPage","variables":{"page":1,"role":"python-developer"},"query":"query SeoLandingRoleSearchPage($role: String!, $page: Int!) {\n  talent {\n    seoLandingPageJobSearchResults(role: $role, page: $page) {\n      totalStartupCount\n      totalJobCount\n      perPage\n      pageCount\n      startups {\n        ...SeoLandingJobSearchResultsFragment\n        __typename\n      }\n      __typename\n    }\n    seoLandingPageRole(query: $role, page: $page) {\n      id\n      meta {\n        ...MetaTagsFragment\n        __typename\n      }\n      roleKeyword {\n        ...RoleKeywordFragment\n        __typename\n      }\n      roleLinks {\n        ...LinkFragment\n        __typename\n      }\n      roleAndLocationLinks {\n        ...LinkFragment\n        __typename\n      }\n      roleRemoteLinks {\n        ...LinkFragment\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment RoleKeywordFragment on SeoRoleKeyword {\n  id\n  slug\n  displayName\n  __typename\n}\n\nfragment SeoLandingJobSearchResultsFragment on StartupResult {\n  id\n  badges {\n    id\n    name\n    label\n    tooltip\n    avatarUrl\n    rating\n    __typename\n  }\n  companySize\n  highConcept\n  highlightedJobListings {\n    autoPosted\n    atsSource\n    description\n    jobType\n    liveStartAt\n    locationNames\n    primaryRoleTitle\n    remote\n    slug\n    title\n    compensation\n    ...SavedJobListingSearchResultFragment\n    __typename\n  }\n  logoUrl\n  name\n  slug\n  __typename\n}\n\nfragment SavedJobListingSearchResultFragment on JobListingSearchResult {\n  id\n  isBookmarked\n  __typename\n}\n\nfragment LinkFragment on Link {\n  label\n  url\n  __typename\n}\n\nfragment MetaTagsFragment on MetaTags {\n  canonicalUrl\n  description\n  image\n  ogUrl\n  robots\n  structuredData\n  title\n  type\n  __typename\n}\n"}
            data = {"operationName":"SeoLandingLocationSearchPage","variables":{"location":location,"page":self.page_number},"query":"query SeoLandingLocationSearchPage($location: String!, $page: Int!) {\n  talent {\n    seoLandingPageJobSearchResults(location: $location, page: $page) {\n      totalStartupCount\n      totalJobCount\n      perPage\n      pageCount\n      startups {\n        ...SeoLandingJobSearchResultsFragment\n        __typename\n      }\n      __typename\n    }\n    seoLandingPageLocation(query: $location, page: $page) {\n      id\n      meta {\n        ...MetaTagsFragment\n        __typename\n      }\n      newTag {\n        ...LocationNewTagFragment\n        __typename\n      }\n      roleAndLocationLinks {\n        ...LinkFragment\n        __typename\n      }\n      roleRemoteLinks {\n        ...LinkFragment\n        __typename\n      }\n      locationLinks {\n        ...LinkFragment\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment LocationNewTagFragment on NewTag {\n  id\n  slug\n  displayName\n  __typename\n}\n\nfragment SeoLandingJobSearchResultsFragment on StartupResult {\n  id\n  badges {\n    id\n    name\n    label\n    tooltip\n    avatarUrl\n    rating\n    __typename\n  }\n  companySize\n  highConcept\n  highlightedJobListings {\n    autoPosted\n    atsSource\n    description\n    jobType\n    liveStartAt\n    locationNames\n    primaryRoleTitle\n    remote\n    slug\n    title\n    compensation\n    ...SavedJobListingSearchResultFragment\n    __typename\n  }\n  logoUrl\n  name\n  slug\n  __typename\n}\n\nfragment SavedJobListingSearchResultFragment on JobListingSearchResult {\n  id\n  isBookmarked\n  __typename\n}\n\nfragment LinkFragment on Link {\n  label\n  url\n  __typename\n}\n\nfragment MetaTagsFragment on MetaTags {\n  canonicalUrl\n  description\n  image\n  ogUrl\n  robots\n  structuredData\n  title\n  type\n  __typename\n}\n"}
            yield scrapy.Request(url, method="POST", headers=self.headers, meta={'location':location}, body=json.dumps(data))
        

    def parse(self, response):
        res = response.json()
        startups = res['data']['talent']['seoLandingPageJobSearchResults'].get('startups')
        pageCount = res['data']['talent']['seoLandingPageJobSearchResults'].get('pageCount')

        for s in startups:
            company_name = s['name']
            company_desc = s['highConcept']
            highlightedJobListings = s.get('highlightedJobListings')
            company_logoUrl = s['logoUrl']
            for job in highlightedJobListings:
                job_description = job['description']
                jobType = job['jobType']
                liveStartAt = job['liveStartAt']
                locationNames_country = job['locationNames']
                primaryRoleTitle = job['primaryRoleTitle']
                title = job['title']
                remote = str(job['remote']) #True
                slug = job['slug']
                compensation = job['compensation']
                _id = job['id']
            

                yield {
                    'created_at': get_current_date(),
                    'id': _id,
                    'company_name': company_name,
                    'job_title': title,
                    'country':','.join(locationNames_country),
                    'city': '',
                    'job_application_url':'',
                    'job_description': job_description,
                    'job_type':  jobType,
                    'min_salary': '',
                    'max_salary': '',
                    'fixed_salay': '',
                    'salary_currency': 'USD',
                    'division':'',
                    'company_logo': company_logoUrl,
                    'salary_interval': '',
                    'remote_work_policy': remote,
                    'company_bio': company_desc,
                }
                gc.collect()
        self.page_number += 1
        location = response.meta['location']
        if self.page_number <= pageCount:
            url = 'https://angel.co/graphql?fallbackAOR=talent'
            data = {"operationName":"SeoLandingLocationSearchPage","variables":{"location":location,"page":self.page_number},"query":"query SeoLandingLocationSearchPage($location: String!, $page: Int!) {\n  talent {\n    seoLandingPageJobSearchResults(location: $location, page: $page) {\n      totalStartupCount\n      totalJobCount\n      perPage\n      pageCount\n      startups {\n        ...SeoLandingJobSearchResultsFragment\n        __typename\n      }\n      __typename\n    }\n    seoLandingPageLocation(query: $location, page: $page) {\n      id\n      meta {\n        ...MetaTagsFragment\n        __typename\n      }\n      newTag {\n        ...LocationNewTagFragment\n        __typename\n      }\n      roleAndLocationLinks {\n        ...LinkFragment\n        __typename\n      }\n      roleRemoteLinks {\n        ...LinkFragment\n        __typename\n      }\n      locationLinks {\n        ...LinkFragment\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment LocationNewTagFragment on NewTag {\n  id\n  slug\n  displayName\n  __typename\n}\n\nfragment SeoLandingJobSearchResultsFragment on StartupResult {\n  id\n  badges {\n    id\n    name\n    label\n    tooltip\n    avatarUrl\n    rating\n    __typename\n  }\n  companySize\n  highConcept\n  highlightedJobListings {\n    autoPosted\n    atsSource\n    description\n    jobType\n    liveStartAt\n    locationNames\n    primaryRoleTitle\n    remote\n    slug\n    title\n    compensation\n    ...SavedJobListingSearchResultFragment\n    __typename\n  }\n  logoUrl\n  name\n  slug\n  __typename\n}\n\nfragment SavedJobListingSearchResultFragment on JobListingSearchResult {\n  id\n  isBookmarked\n  __typename\n}\n\nfragment LinkFragment on Link {\n  label\n  url\n  __typename\n}\n\nfragment MetaTagsFragment on MetaTags {\n  canonicalUrl\n  description\n  image\n  ogUrl\n  robots\n  structuredData\n  title\n  type\n  __typename\n}\n"}
            yield scrapy.Request(url, method="POST", callback=self.parse, meta={'location':location}, headers=self.headers, body=json.dumps(data))
            
    
settings_file_path = 'lazy_crawler.crawler.settings'
os.environ.setdefault('SCRAPY_SETTINGS_MODULE', settings_file_path)
process = CrawlerProcess(get_project_settings())  
process.crawl(LazyCrawler)
process.start() # the script will block here until the crawling is finished
