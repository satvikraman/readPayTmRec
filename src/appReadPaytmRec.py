import logging
import dotenv
import os
import datetime
from dateutil.relativedelta import relativedelta
import re
import shutil
import sys
import time
import urllib.request
import configparser
import requests

sys.path.append('./src/common')
from paytmTradingIdeas import paytmTradingIdeas
from persistence import persistence

# Reommendation Status transitions as 
# OPEN --> CLOSE

class app():
    def __init__(self, configFile, dbInv=None, dbIntraDay=None, dbFnO=None):
        if(os.path.isfile(configFile)):
            self.__config = configparser.ConfigParser()
            self.__config.read(configFile)
            if(self.__config['APP']['LOG_LEVEL'] == 'DEBUG'):
                level = logging.DEBUG
            elif(self.__config['APP']['LOG_LEVEL'] == 'INFO'):
                level = logging.INFO
            elif(self.__config['APP']['LOG_LEVEL'] == 'WARNING'):
                level = logging.WARNING
            elif(self.__config['APP']['LOG_LEVEL'] == 'ERROR'):
                level = logging.ERROR
            elif(self.__config['APP']['LOG_LEVEL'] == 'CRITICAL'):
                level = logging.CRITICAL
            self.__logger = logging.getLogger(__name__)
            self.__logger.setLevel(level)
    
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fileHandler = logging.FileHandler(filename=self.__config['LOGGING']['LOG_FILE'], mode='w')
            consoleHandler = logging.StreamHandler()
            fileHandler.setFormatter(formatter)
            consoleHandler.setFormatter(formatter)
            logging.getLogger('').addHandler(consoleHandler)
            logging.getLogger('').addHandler(fileHandler)

            if dbInv == None:
                dbInv = self.__config['DATABASE']['DB_EQUITY']
            self.__backupDb(dbInv)                
            self.__persistenceInv = persistence(configFile, dbInv)

            if dbFnO == None:
                dbFnO = self.__config['DATABASE']['DB_FNO']
            self.__backupDb(dbFnO)                
            self.__persistenceFnO = persistence(configFile, dbFnO)

            self.__paytm = paytmTradingIdeas(configFile)

            self.__numRetries = int(self.__config['APP']['NUM_RETRIES'])
            self.__paytmBaseURL = self.__config['APP']['PATYM_URI']

            # Download the latest Paytm dataset once every day
            dotenv.load_dotenv('.env', override=True)
            paytm_dataset_valid_until_date = os.environ.get('paytm_dataset_valid_until_date', '')
            self.__today = datetime.datetime.today().strftime("%d-%b-%Y")
            if(paytm_dataset_valid_until_date.upper() != self.__today.upper()):
                try:
                    for dataset in ['PAYTM_EQUITY_DATASET', 'PAYTM_FUTURE_DATASET', 'PAYTM_OPTION_DATASET']:
                        url = self.__config['PAYTM'][dataset]
                        paytmDataset = self.__config['PAYTM']['DATASET_PATH'] + re.sub(r'^.*/', '', url)
                        urllib.request.urlretrieve(url, paytmDataset)
                except Exception as e:
                    self.__logger.critical(e)
                finally:
                    dotenv.set_key('./.env', "paytm_dataset_valid_until_date", self.__today)


    def __backupDb(self, db):
        dbName = re.sub(r'^.*/', '', db)
        dbName = re.sub(r'.json', '', dbName)
        backupDb = './db/backup/' + dbName + '-APP-' + datetime.datetime.today().strftime("%d-%b-%Y-%H-%M-%S") + '.json'
        self.__logger.info("Backing up DB as %s", backupDb)
        shutil.copyfile(db, backupDb)


    def __send2PayTm(self, endPoint, recDict):
        if recDict == None:
            return True
        
        retries = self.__numRetries
        status = False
        while not status and retries >= 0:
            try:
                url = self.__paytmBaseURL
                if endPoint == 'NEW_REC':
                    url = url + 'v1/rec'
                    res = requests.post(url, json=recDict)
                elif endPoint == 'UPDATE_REC':
                    url = url + 'v1/rec'
                    res = requests.put(url, json=recDict)
                elif endPoint == 'VISIBILITY':
                    url = url + 'v1/visibility'
                    res = requests.put(url, json=recDict)
                if int(res.status_code / 100) == 2:
                    status = True
                else:
                    self.__logger.error("Unable to send request to PayTm service. Attempt %d of %d: %s", self.__numRetries-retries, self.__numRetries, recDict)
                    retries -= 1
            except Exception as e:
                self.__logger.error("Exception: %s. Attempt %d of %d: %s", e, self.__numRetries-retries, self.__numRetries, recDict)
                retries -= 1

        return status


    def __hasChanged(self, dbDict, rowDict):
        status = False
        if rowDict['REC_STATUS'] == 'CLOSE' and dbDict['REC_STATUS'] != 'CLOSE':
            status = True

        keysToCheck = ['TARGET', 'STOP_LOSS', 'LOW_REC_PRICE', 'HIGH_REC_PRICE']
        for keyToCheck in keysToCheck:
            if rowDict[keyToCheck] != dbDict[keyToCheck]:
                status = True

        return status


    def __updateRecStatus(self, rowDict, product):
        rowDict['VISIBLE'] = 'VISIBLE'
        persistence = self.__persistenceInv if product == 'EQUITY' else self.__persistenceFnO

        # Find open recommendations matching the condition in DB
        self.__logger.debug("updateRecStatus: Finding in DB nseSym=%s, strategy=%s, date=%s, time=%s, recStatus=%s", 
                            rowDict['MKT_SYMBOL'], rowDict['STRATEGY'], rowDict['REC_DATE'], rowDict['REC_TIME'], 'None')
        isInDb, dbDict = persistence.isInDb([['MKT_SYMBOL', rowDict['MKT_SYMBOL']], ['STRATEGY', rowDict['STRATEGY']], ['REC_DATE', rowDict['REC_DATE']]])
        self.__logger.debug("Find results: status = %s & dbDict = %s", isInDb, dbDict)

        # If no recommendation found in DB and if the current recommendation is not close, then
        # Insert the recommendation in DB
        if not isInDb:
            if (rowDict['REC_STATUS'] != 'CLOSE'):
                recDict = self.__paytm.prepareRecDict(rowDict)                                
                status = self.__send2PayTm('NEW_REC', recDict)
                rowDict['ACK'] = 'ACK' if status else 'NACK'
                persistence.insertDb(rowDict, [['MKT_SYMBOL', rowDict['MKT_SYMBOL']], ['STRATEGY', rowDict['STRATEGY']], ['REC_DATE', rowDict['REC_DATE']], ['REC_TIME', rowDict['REC_TIME']]])
                self.__logger.info('New Recommendation %s', rowDict)
            else:
                rowDict['ACK'] = 'ACK'
                res = persistence.insertDb(rowDict, [['MKT_SYMBOL', rowDict['MKT_SYMBOL']], ['STRATEGY', rowDict['STRATEGY']], ['REC_DATE', rowDict['REC_DATE']], ['REC_TIME', rowDict['REC_TIME']]])
                self.__logger.info("Recommendation for %s is new (i.e. not in DB) but is already closed %s", rowDict['MKT_SYMBOL'], rowDict)
        elif isInDb:
                # If the recommendation has changed then
                isChange = self.__hasChanged(dbDict, rowDict)

                if isChange:
                    recDict = self.__paytm.prepareRecDict(rowDict)
                    status = self.__send2PayTm('UPDATE_REC', recDict)
                    rowDict['ACK'] = 'ACK' if status else 'NACK'
                    persistence.updateDb(rowDict, [['MKT_SYMBOL', rowDict['MKT_SYMBOL']], ['STRATEGY', rowDict['STRATEGY']], ['REC_DATE', rowDict['REC_DATE']], ['REC_TIME', rowDict['REC_TIME']]])
                    self.__logger.info('Updated Recommendation %s', rowDict)
                #else: Nothing to be done


    def __updateMismatchedVisibilityRecs(self, product):
        visibilityDict = {'SOURCE': 'PAYTM', 'VISIBLE': []}
        persistence = self.__persistenceInv if product == 'EQUITY' else self.__persistenceFnO
        # Find all strategyToCheck (MARGIN|OPTIONS|FUTURE) recommendations in DB that are not closed
        dbDicts = persistence.getDb([['REC_STATUS', '!CLOSE']])

        # If they are not found in the recommendations on the web page --> close them 
        for dbDict in dbDicts:
            visible = self.__paytm.isVisible(dbDict['STOCK'], dbDict['STRATEGY'], dbDict['REC_DATE'])

            # Close the recommendation that was not found
            if visible:
                val = dbDict['MKT_SYMBOL'] + '-' + dbDict['STRATEGY'] + '-' + dbDict['REC_DATE'] + '-' + dbDict['REC_TIME']
                visibilityDict['VISIBLE'].append(val)
                if (dbDict['VISIBLE'] != 'VISIBLE'):
                    dbDict['VISIBLE'] = 'VISIBLE'
                    persistence.updateDb(dbDict, [['MKT_SYMBOL', dbDict['MKT_SYMBOL']], ['STRATEGY', dbDict['STRATEGY']], ['REC_DATE', dbDict['REC_DATE']], ['REC_TIME', dbDict['REC_TIME']]])
                    self.__logger.info("Changing rec's visibility to visible => %s", dbDict)
            elif (dbDict['VISIBLE'] == 'VISIBLE') or dbDict['REC_STATUS'] != 'CLOSE':
                dbDict['VISIBLE'] = 'HIDDEN'
                dbDict['REC_STATUS'] = 'CLOSE'
                dbDict['REC_CLOSE_DATE'] = self.__today
                dbDict['CLOSE_PRICE'] = dbDict['HIGH_REC_PRICE']
                persistence.updateDb(dbDict, [['MKT_SYMBOL', dbDict['MKT_SYMBOL']], ['STRATEGY', dbDict['STRATEGY']], ['REC_DATE', dbDict['REC_DATE']], ['REC_TIME', dbDict['REC_TIME']]])
                self.__logger.info("Changing the visibility to hidden and closing the rec => %s", dbDict)

        self.__logger.critical("Sending of visiblilityDict from PaytmTradingIdea to Paytm Broker is disabled")
        #if len(visibilityDict['VISIBLE']) > 0:
        #    #self.__send2PayTm('VISIBILITY', visibilityDict)
        #    #self.__logger.info('Visibility %s', visibilityDict)


    def __sendNonAckedRecsFromDb(self, product):
        # Find open recommendations matching the condition in DB
        self.__logger.debug("__sendNonAckedRecs: Finding in DB ACK=False")
        persistence = self.__persistenceInv if product == 'EQUITY' else self.__persistenceFnO

        dbDicts = persistence.getDb([['ACK', '!ACK']])
        self.__logger.debug("Find results: dbDict = %s", dbDicts)

        for dbDict in dbDicts:
            recDict = self.__paytm.prepareRecDict(dbDict)
            status = self.__send2PayTm('UPDATE_REC', recDict)
            dbDict['ACK'] = 'ACK' if status else 'NACK'
            persistence.updateDb(dbDict, [['MKT_SYMBOL', dbDict['MKT_SYMBOL']], ['STRATEGY', dbDict['STRATEGY']], ['REC_DATE', dbDict['REC_DATE']], ['REC_TIME', dbDict['REC_TIME']]])
            self.__logger.info('NACK Recommendation %s', dbDict)

    def runPeriodicChecks(self):
        # Refresh webpage and find new ideas
        for product in ['EQUITY', 'DERIVATIVES']:            
            self.__paytm.setProduct(product)
            # Send all recommendations in DB that haven't be ACK'ed
            self.__sendNonAckedRecsFromDb(product)
            self.__paytm.refreshIdeas()
            self.__paytm.scrapeIdeas()
            for invRecDict in self.__paytm.getNextPaytmTblRow():
                self.__updateRecStatus(invRecDict, product)
        
    def runPostMarketCloseChecks(self):
        self.__logger.info("Checking for mismatched visibility")
        for product in ['EQUITY', 'DERIVATIVES']:
            self.__paytm.setProduct(product)
            self.__updateMismatchedVisibilityRecs(product)


    def openPaytmSession(self):
        self.__paytm.browsePaytm()


if __name__ == '__main__':
    trade = app('./paytm.ini')
    trade.openPaytmSession()

    marketClose = False
    while not marketClose:
        marketClose = datetime.datetime.now() >= datetime.datetime.now().replace(hour=15, minute=30)
        trade.runPeriodicChecks()
        time.sleep(5)
    
    time.sleep(60)
    if marketClose:
        trade.runPostMarketCloseChecks()