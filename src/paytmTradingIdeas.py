import logging
import csv
import dotenv
import os
import re
import sys
import time
import datetime
from dateutil.relativedelta import relativedelta
import configparser
import urllib

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

sys.path.append('./src/common')
from pushbullet import PushBullet
from googleWorkspace import googleWorkspace

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from breeze_connect import BreezeConnect

class paytmTradingIdeas():
    def __init__(self, configFile):
        if(os.path.isfile(configFile)):
            self.__config = configparser.ConfigParser()
            self.__config.read(configFile)
            
            if(self.__config['PAYTM']['LOG_LEVEL'] == 'DEBUG'):
                level = logging.DEBUG
            elif(self.__config['PAYTM']['LOG_LEVEL'] == 'INFO'):
                level = logging.INFO
            elif(self.__config['PAYTM']['LOG_LEVEL'] == 'WARNING'):
                level = logging.WARNING
            elif(self.__config['PAYTM']['LOG_LEVEL'] == 'ERROR'):
                level = logging.ERROR
            elif(self.__config['PAYTM']['LOG_LEVEL'] == 'CRITICAL'):
                level = logging.CRITICAL
            self.__logger = logging.getLogger(__name__)
            self.__logger.setLevel(level)
            self.__iclick2GainDict = {}
            self.__iclick2InvestDict = {}
            self.__pushbullet = None
            self.__google = None

            # Connect w/ the browser driver
            self.__browserEngine = self.__config['DEFAULT']['BROWSER']
            if self.__browserEngine == 'CHROME':
                self.__browserDriver = self.__config['DEFAULT']['CHROME_DRIVER']
            elif self.__browserEngine == 'EDGE':
                self.__browserDriver = self.__config['DEFAULT']['EDGE_DRIVER']

            # Open ICICI Direct and let the user login
            if self.__browserEngine == 'CHROME':
                self.__browser = webdriver.Chrome(self.__browserDriver)
            else:
                self.__browser = webdriver.Edge(self.__browserDriver)


            # Initialize PushBullet to enable mobile notifications
            if self.__config['APP']['USE_PUSHBULLET'] == 'YES':
                if self.__pushbullet == None:
                    dotenv.load_dotenv('./.env', override=True)
                    pb_api_key = os.environ.get('pb_api_key', '')

                    self.__pushbullet = PushBullet(pb_api_key)
                    self.__pushbulletDev = self.__pushbullet.getDevices()

                # Connect to Google sheets
            if self.__config['APP']['USE_SPREADSHEET'] == 'YES':
                if self.__google == None:
                    spreadsheetID = self.__config['APP']['SPREADSHEET_ID']
                    sheetName = self.__config['APP']['SHEET_NAME']
                    self.__google = googleWorkspace(spreadsheetID, sheetName)
                    self.__google.authorize()
                    self.__google.buildSheets()
                    self.__google.buildDrive()


    def __handleException(self, e):
        pattern = r".*(disconnected: not connected to DevTools|no such window)"
        if re.match(pattern,  str(e), re.IGNORECASE):
            self.__logger.critical("ERROR: %s", e)
            self.__logger.critical("EXITING")            
            assert(False)
        else:
            self.__logger.error("ERROR: %s", e)
        time.sleep(1)


    def __getWebElement(self, xpath, check, singular=True):
        nextStep = False
        attempts = 0
        element = None
        elements = []
        while not nextStep and attempts < 3:
            try:
                if check == 'PRESENCE':
                    if singular:
                        element = WebDriverWait(self.__browser, 5).until(EC.presence_of_element_located((By.XPATH, xpath)))
                    else:
                        elements = WebDriverWait(self.__browser, 5).until(EC.presence_of_all_elements_located((By.XPATH, xpath)))
                elif check == 'VISIBILITY':
                    if singular:
                        element = WebDriverWait(self.__browser, 5).until(EC.visibility_of_element_located((By.XPATH, xpath)))
                    else:
                        elements = WebDriverWait(self.__browser, 5).until(EC.visibility_of_all_elements_located((By.XPATH, xpath)))
                elif check == 'CLICKABLE':
                    element = WebDriverWait(self.__browser, 5).until(EC.element_to_be_clickable((By.XPATH, xpath)))
                    element.click()
                    time.sleep(5)
                else:
                    assert(False)
                nextStep = True
            except Exception as e:
                attempts += 1
                self.__handleException(e)
        
        return element if singular else elements


    def __enterPasscode(self):
        if "passcode" in self.__browser.current_url:
            ifEnterPasscode = self._paytmTradingIdeas__getWebElement("//*[@id='newroot']/div/div/div/div/div/div[1]/div[1]/div/div[2]/div/div[1]", 'PRESENCE')
            if ifEnterPasscode != None:
                passcodeIn = self._paytmTradingIdeas__getWebElement("//*[@id='newroot']/div/div/div/div/div/div[1]/div[1]/div/div[2]/div/div[2]/div/div/div/input", 'PRESENCE', singular=False)
                passcode = os.environ.get('paytm_passcode', '')
                for i in range(len(passcode)):
                    passcodeIn[i].send_keys(int(passcode[i]))
        time.sleep(5)            


    def scrapeIdeas(self):
        scrapeAttempt = 0
        while scrapeAttempt < 3: 
            try:
                self.__paytmTblRows = self.__getWebElement("//*[@id='mainApp']/div/div[3]/div[2]/div", 'VISIBILITY', singular=False) 
                self.__enterPasscode()
                break
            except Exception as err:
                self.__logger.error('Unable to scrape ideas %s', err)
                scrapeAttempt += 1


    def refreshIdeas(self):
        if self.__browser.current_url != self.__config['PAYTM']['PAYTM_TRADING_IDEA_URL']:
            self.__browseTradingIdeas()
        else:
            self.__browser.refresh()
        self.__getWebElement("//*[@id='mainApp']/div/div[1]/div[2]/div/div[2]/span", 'CLICKABLE')


    def __loginPaytm(self):
        # Enter OTP
        uid = os.environ.get('paytm_mobile', '')
        mobile = self.__getWebElement("//*[@id='root']/div/div[2]/div/div[1]/div[1]/div/div[2]/fieldset/input", 'PRESENCE')
        mobile.send_keys(uid)
        self._paytmTradingIdeas__getWebElement("//*[@id='root']/div/div[2]/div/div[1]/div[1]/div/div[2]/span[2]/button", 'CLICKABLE')
        self.__google.writeToCell('A1', 'B4', [[' ', ' '], [' ', ' '], [' ', ' '], [' ', ' ']])
        self.__google.writeToCell('C3', 'C3', [[' ']])
        self.__google.writeToCell('A3', 'A3', [['Enter the 6 digit OTP']])
        self.__google.writeToCell('A4', 'A4', [['Resend OTP']])
        OTPnotrecv = True
        while OTPnotrecv:
            status, value = self.__google.readFromCell('B3', 'C3')
            if status and len(value[0]) == 2 and len(value[0][0]) == 6 and value[0][1].upper() == 'YES': 
                OTPnotrecv = False
            else:
                # Resend OTP
                status, value = self.__google.readFromCell('B4', 'B4')
                if status and value[0][0].upper() == 'YES':
                    self._paytmTradingIdeas__getWebElement("//*[@id='root']/div/div[2]/div/div[1]/div[1]/div/div[3]/div[2]/span/div/span", 'CLICKABLE')
                    self.__google.writeToCell('B4', 'B4', [[' ']])
                time.sleep(1)
        otpIn = self._paytmTradingIdeas__getWebElement("//*[@id='root']/div/div[2]/div/div[1]/div[1]/div/div[2]/div/div/div/input", 'PRESENCE', singular=False)
        for i in range(len(value[0][0])):
            otpIn[i].send_keys(int(value[0][0][i]))
        self._paytmTradingIdeas__getWebElement("//*[@id='root']/div/div[2]/div/div[1]/div[1]/div/div[4]/span/button", 'CLICKABLE')
        self.__enterPasscode()            


    def __setEquityFilters(self):
        # Click Equity
        self.__getWebElement("//*[@id='newroot']/div/div/div[4]/div[2]/div/div[2]/button", 'CLICKABLE')
        # Filter on created time
        self.__getWebElement("//*[@id='mainApp']/div/div[2]/div/div[2]/div[1]/div[1]/div", 'CLICKABLE')
        self.__getWebElement("//*[@id='mainApp']/div/div[2]/div/div[2]/div[1]/div[3]/div/div/div/div/div/div/div[2]/div/div[2]", 'CLICKABLE')
        # Enable viewing both open and close recs
        self.__getWebElement("//*[@id='mainApp']/div/div[2]/div/div[2]/div[2]/div[1]/div", 'CLICKABLE')
        self.__getWebElement("//*[@id='mainApp']/div/div[2]/div/div[2]/div[2]/div[3]/div/div/div/div/div/div/div[2]/div[1]/div[1]/div/div/div[2]/div[1]", 'CLICKABLE')
        self.__getWebElement("//*[@id='mainApp']/div/div[2]/div/div[2]/div[2]/div[3]/div/div/div/div/div/div/div[2]/div[1]/div[2]/div/div/div[2]/div[1]", 'CLICKABLE')
        # Click on Equity again
        self.__getWebElement("//*[@id='newroot']/div/div/div[4]/div[2]/div/div[2]/button", 'CLICKABLE')

    def __browseTradingIdeas(self):
        self.__browser.get(self.__config['PAYTM']['PAYTM_TRADING_IDEA_URL'])
        # Check if the passcode needs to be entered
        self.__enterPasscode()
        # Check if the trading ideas is live banner is being shown
        tradingIdeasIsLive = self.__getWebElement("//*[@id='newroot']/div/div/div[6]/div[2]/div/div[3]/div[1]", 'PRESENCE')
        if tradingIdeasIsLive != None:
            clickBody = body = self._paytmTradingIdeas__getWebElement("/html/body", 'PRESENCE')
            clickBody.click()


    def browsePaytm(self):        
        self.__browser.get(self.__config['PAYTM']['PAYTM_REC_URL'])
        if self.__google != None:
            self.__loginPaytm()
            self.__browseTradingIdeas()
        else:
            input("Wait for the user to login")
        self.__setEquityFilters()
        

    def closeBrowser(self):  
        self.__browser.quit()


    def mapPaytmStockToMktSymbol(self, stkName, strategy="EQ"):
        status = False
        rowDict = {'SECURITY_ID': '', 'MKT': '', 'MKT_SYMBOL': ''}
        # Equity investment. Could be intraday as well
        datasets = [[self.__config['PAYTM']['NSE_DATASET'], 'NSE', ['security_id', 'symbol', 'name', 'exchange', 'series']]]

        for dataset in datasets:
            with(open(dataset[0], 'r')) as paytmcsv:
                paytmReader = csv.DictReader(paytmcsv)
                for paytmRow in paytmReader:
                    if paytmRow[dataset[2][2]].upper() == stkName.upper() and paytmRow[dataset[2][3]].upper() == "NSE" and paytmRow[dataset[2][4]].upper() == "EQ" :
                        status = True
                        rowDict['SECURITY_ID'] = paytmRow[dataset[2][0]]
                        rowDict['MKT'] = 'NSE'
                        rowDict['MKT_SYMBOL'] = paytmRow[dataset[2][1]]
                        break
            if status:
                break

        self.__logger.debug('Generated dictionary %s', rowDict)
        return status, rowDict['SECURITY_ID'],  rowDict['MKT_SYMBOL'], rowDict['MKT']
    

    def __suggestInvPeriod(self, strategy, iciciSymbol, recDate):
        invPeriod = ''
        if strategy == 'MARGIN':
            invPeriod  = '0 DAYS'
        elif strategy == 'OPTIONS':
            spliticiciSymbol = iciciSymbol.split('-')
            expiryDate = spliticiciSymbol[1]+'-'+spliticiciSymbol[2]+'-'+spliticiciSymbol[3]
            recDate    = datetime.datetime.strptime(recDate, "%d-%b-%Y")
            expDate    = datetime.datetime.strptime(expiryDate, "%d-%b-%Y")
            invPeriod  = (expDate - recDate).days
            invPeriod  = str(invPeriod) + ' ' + 'DAYS*'
        else:
            invDays = invMonths = 0
            if strategy == 'MOMENTUM PICK':
                invPeriod  = '14 DAYS*'
                invDays    = 14
            elif strategy == 'QUANT PICKS':
                invPeriod  = '3 MONTHS*'
                invMonths  = 3
            elif strategy == 'GLADIATOR STOCKS':
                invPeriod  = '3 MONTHS*'
                invMonths  = 3
            else:
                invPeriod  = '14 DAYS*'
                invDays    = 14
                self.__logger.error("Handle suggestion of investment period for this strategy %s", strategy)
            expDate = datetime.datetime.strftime(datetime.datetime.strptime(recDate, '%d-%b-%Y') + relativedelta(days=invDays, months=invMonths), '%d-%b-%Y')
        return invPeriod, expDate


    def isVisible(self, source, iciciSymbol, strategy, buySell):
        visible = False
        if source == 'iCLICK-2-GAIN':
            key = (iciciSymbol, strategy, buySell)
            if key in self.__iclick2GainDict:
                visible = self.__iclick2GainDict[key]['VISIBLE'] == 'VISIBLE'
        else:
            key = (iciciSymbol, strategy)
            if key in self.__iclick2InvestDict:
                visible = self.__iclick2InvestDict[key]['VISIBLE'] == 'VISIBLE'
        return visible


    def prepareRecDict(self, rowDict):
        mandatoryKeys = ['STOCK', 'SOURCE', 'MKT_SYMBOL', 'SECURITY_ID', 'ICICI_SYMBOL', 'STRATEGY', 'BUY_SELL', 'REC_DATE', 'REC_STATUS', 'EXP_DATE', 'VISIBLE']
        mandatoryPriceKeys = ['LOW_REC_PRICE', 'HIGH_REC_PRICE', 'TARGET', 'STOP_LOSS']
        mandatoryDervKeys = ['LOT_SIZE']
        mandatoryLevKeys = ['REC_TIME']
        
        importantKeys = ['INV_PERIOD', 'MKT']
        priceKeys = ['CMP', 'PART_PROFIT_PRICE', 'FINAL_PROFIT_PRICE', 'EXIT_PRICE']
        
        otherLevkeys = ['PART_PROFIT_PERC', 'UPDATE_ACTION_1', 'UPDATE_TIME_1', 'UPDATE_ACTION_2', 'UPDATE_TIME_2']
        otherNonLevkeys = otherLevkeys + ['REC_TIME']
        
        recDict = {}

        if rowDict['STRATEGY'] == 'OPTIONS':
            keysToSend = mandatoryKeys + mandatoryPriceKeys + mandatoryDervKeys + mandatoryLevKeys + importantKeys + priceKeys + otherLevkeys
        elif rowDict['STRATEGY'] == 'MARGIN':
            keysToSend = mandatoryKeys + mandatoryPriceKeys                     + mandatoryLevKeys + importantKeys + priceKeys + otherLevkeys
        else:
            keysToSend = mandatoryKeys + mandatoryPriceKeys                                        + importantKeys + priceKeys + otherNonLevkeys

        for key in keysToSend:
            if key in rowDict:
                recDict[key] = rowDict[key]
            elif key in mandatoryKeys + mandatoryPriceKeys + mandatoryDervKeys:
                self.__logger.critical("Mandatory key %s missing. Sending empty dict", key)
                return {}
            elif rowDict['STRATEGY'] in ['OPTIONS', 'MARGIN'] and key in mandatoryLevKeys:
                self.__logger.critical("Mandatory key %s missing. Sending empty dict", key)
                return {}
            elif key in importantKeys:
                if key == 'INV_PERIOD':
                    recDict['INV_PERIOD'], _ = self.__suggestInvPeriod(rowDict['STRATEGY'], rowDict['ICICI_SYMBOL'], rowDict['REC_DATE'])
            elif key in priceKeys:
                recDict[key] = 0
            elif key in otherNonLevkeys:
                recDict[key] = ''        
        return recDict


    def __convPriceToFloat(self, priceStr):
        priceStr = re.sub(r',|-|\s+', '', priceStr)
        price = float(priceStr) if priceStr != '' else 0
        return price


    def analystToInvest(self, analyst, product='EQUITY', buySell='BUY'):
        status = False
        allAnalysts = ['LOTUS FUNDS', 'MANISH SHAH', 'MADHU BANSAL', 'KAVAN PATEL', 'KUSH BOHRA', 'DHWANI PATEL' , 'CLOVEK WEALTH', 'ABHIKUMAR PATEL']
        analystToInvest = ['LOTUS FUNDS', 'CLOVEK WEALTH']

        if product == 'EQUITY':
            if analyst.upper() in analystToInvest:
                status = True
        
        return status
    

    def __formatPaytmTblRowToDict(self, tblRow):
        rowDict = None
        self.__logger.debug('==== Format Table Row To Dictionary ====')
        
        # Find the strategy
        analyst = tblRow.find_element_by_class_name("o3dmU").text
        analyst = re.sub('Powered By ', '', analyst, flags=re.IGNORECASE)
        # TODO: figure out if it is intraday, short term or long term
        if self.analystToInvest(analyst):
            # Get the stock name
            stockName = tblRow.find_element_by_class_name("nGNYx").text
            # Find the securityID, mktSymbol etc... 
            status, securityID, mktSymbol, mkt = self.mapPaytmStockToMktSymbol(stockName)
            # If found, process remaining keys required in the dictionary
            if status:
                rowDict = {}
                rowDict['STOCK'] = stockName
                rowDict['SECURITY_ID'] = securityID
                rowDict['MKT_SYMBOL'] = mktSymbol
                rowDict['MKT'] = mkt
                rowDict['STRATEGY'] = tblRow.find_element_by_class_name("AftEd").text
                rowDict['STRATEGY'] = analyst + '-' + rowDict['STRATEGY']
                rowDict['INV_PERIOD'] = '12 MONTHS'
                rowDict['BUY_SELL'] = tblRow.find_element_by_class_name("AsZN3").text
                rowDict['CMP'] = tblRow.find_element_by_class_name("YujWg").text
                rowDict['CMP'] = re.sub(r'\n.*$', '', rowDict['CMP'])
                rowDict['LOW_REC_PRICE'] = rowDict['HIGH_REC_PRICE'] = tblRow.find_element_by_class_name("x3qrI").text
                dateAndTime = tblRow.find_element_by_class_name("c31Md").text
                rowDict['REC_DATE'] = dateAndTime.split(' ')[0]
                rowDict['REC_TIME'] = dateAndTime.split(' ')[1]
                rowDict['TARGET'] = tblRow.find_element_by_class_name("dZwGK").text
                rowDict['STOP_LOSS'] = tblRow.find_element_by_class_name("Y7pkW").text
                rowDict['STATUS'] = tblRow.find_element_by_class_name("LvNsX").text
                rowDict['SOURCE'] = 'PAYTM'

        return rowDict


    def getNextPaytmTblRow(self):
        parseAttempt = 0
        while parseAttempt < 3:
            try:
                for tblRow in self.__paytmTblRows:
                    rowDict = self.__formatPaytmTblRowToDict(tblRow)
                    if rowDict != None:
                        self.__logger.debug('Generated dictionary %s', rowDict)
                        yield rowDict
                break
            except Exception as e:
                self.__logger.error('getNextPaytmTblRow: %s', rowDict)
                self.scrapeIdeas()
                parseAttempt += 1
