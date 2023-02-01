import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import openpyxl

# GoogleSheetsPipeline
class GoogleSheetsPipeline(object):
    
    def __init__(self):
        
        scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/drive.file','https://www.googleapis.com/auth/spreadsheets']
        self.creds = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)

        self.client = gspread.authorize(self.creds)

        self.client = gspread.authorize(self.creds)
        self.sheet = self.client.open("JobScrapeData").sheet1
        self.spreadsheet_key = '1T0Z2nXcrVzj9wxfd6gH8sHt2vpq4fgI2hVgqJoefcFY'
        self.wks_name = 'Sheet1'

    def _append_to_sheet(self, item):
        """Append item to spreadsheet"""
        # Add headings to the sheet if it's the first item
        if self.sheet.row_values(1) != list(item.keys()):
            values = list(item.keys())
            # append body to spreadsheet                
            self.sheet.insert_row(values, index=self.sheet.row_count)

        else:
            # Add the item data to the sheet
            values = list(item.values())
            # append body to spreadsheet
            self.sheet.append_row(values)

    def process_item(self, item,spider):
        cell = self.sheet.find(item['id'])
        if cell:
            print("job is already exists in google sheet.........................!!")
        else:
            print('Now, adding jobs', item)
            time.sleep(5)
            self._append_to_sheet(item)
            return item
        
class ExcelWriterPipeline(object):
    
    def __init__(self):
        # self.created_time = datetime.datetime.now()
        self.wb = openpyxl.Workbook()
        self.ws = self.wb.active
        self.ws.title = "Scraped Data"
        self.row_num = 1  # counter for the current row in the sheet

    def process_item(self, item, spider):
        # Add headings to the sheet if it's the first item
        if self.row_num == 1:
            self.ws.append(list(item.keys())) # convert dict_keys to a list and use it as headings
        # Add the item data to the sheet
        self.ws.append(list(item.values()))
        self.row_num += 1  # increment the row counter

        return item

    def close_spider(self, spider):
        self.wb.save(f"scraped_data.xlsx")  # save the workbook