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

            """
            if dbIntraDay == None:
                dbIntraDay = self.__config['DATABASE']['DB_INTRADAY']
            self.__backupDb(dbIntraDay)                
            self.__persistenceIntraDay = persistence(configFile, dbIntraDay)

            if dbFnO == None:
                dbFnO = self.__config['DATABASE']['DB_FNO']
            self.__backupDb(dbFnO)                
            self.__persistenceFnO = persistence(configFile, dbFnO)
            """

            self.__paytm = paytmTradingIdeas(configFile)

            self.__numRetries = int(self.__config['APP']['NUM_RETRIES'])
            self.__paytmBaseURL = self.__config['APP']['PATYM_URI']
            """
            # Download the latest ICICI dataset once every day
            dotenv.load_dotenv('.env', override=True)
            paytm_dataset_valid_until_date = os.environ.get('paytm_dataset_valid_until_date', '')
            today = datetime.datetime.today().strftime("%d-%b-%Y").upper()
            if(paytm_dataset_valid_until_date.upper() != today):
                paytmDatasetPath = "./dataset/"
                paytmDataset = paytmDatasetPath + "equity_security_master.csv"
                try:
                    urllib.request.urlretrieve(self.__config['PAYTM']['PAYTM_DATASET'], paytmDataset)
                    dotenv.set_key('./.env', "paytm_dataset_valid_until_date", today)
                except Exception as e:
                    self.__logger.critical(e)
            """

    def __backupDb(self, db):
        dbName = re.sub(r'^.*/', '', db)
        dbName = re.sub(r'.json', '', dbName)
        backupDb = './db/backup/' + dbName + '-APP-' + datetime.datetime.today().strftime("%d-%b-%Y-%H-%M-%S") + '.json'
        self.__logger.info("Backing up DB as %s", backupDb)
        shutil.copyfile(db, backupDb)


    def __send2PayTm(self, endPoint, recDict):
        retries = self.__numRetries
        status = False

        while not status and retries >= 0:
            url = self.__paytmBaseURL + 'v1/rec'
            try:
                if endPoint == 'NEW_REC':
                    res = requests.post(url, json=recDict)
                elif endPoint == 'UPDATE_REC':
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


    def __computeExpDate(self, recDict, dbDict):
            status = True
            invDays = invMonths = 0
            invPeriod = recDict['INV_PERIOD']
            if '*' in invPeriod:
                invPeriod = dbDict['INV_PERIOD'] if 'INV_PERIOD' in dbDict else invPeriod

            if 'MONTH'.lower() in invPeriod.lower():
                invMonths = re.match(r'\d+', invPeriod)
                invMonths = int(invMonths.group(0))
            elif 'DAY'.lower() in invPeriod.lower():
                invDays = re.match(r'\d+', invPeriod)
                invDays = int(invDays.group(0))

            expDate = datetime.datetime.strftime(datetime.datetime.strptime(dbDict['REC_DATE'], '%d-%b-%Y') + relativedelta(days=invDays, months=invMonths), '%d-%b-%Y')
            return status, invPeriod, expDate


    def closeExpiredRecs(self, instrument, dryRun=True):
        if instrument == "EQUITY":
            persistence = self.__persistenceInv
        elif instrument == "MARGIN":
            persistence = self.__persistenceIntraDay
        elif instrument == "FnO":
            persistence = self.__persistenceFnO

        dbDicts = persistence.getDb([['REC_STATUS', '!CLOSE'], ['VISIBLE', 'HIDDEN']])
        todaysDate = datetime.datetime.today().date()
        for dbDict in dbDicts:
            expDate = datetime.datetime.strptime(dbDict['EXP_DATE'], '%d-%b-%Y').date()
            if todaysDate >= expDate:
                self.__logger.info("STOCK = %s SOURCE = %s STRATEGY = %s REC_DATE = %s INV_PERIOD = %s EXP_DATE = %s expires today", dbDict['MKT_SYMBOL'], 
                                   dbDict['SOURCE'], dbDict['STRATEGY'], dbDict['REC_DATE'], dbDict['INV_PERIOD'], dbDict['EXP_DATE'])
                if not dryRun:
                    dbDict['REC_STATUS'] = 'CLOSE'
                    recDict = self.__iciciDirect.prepareRecDict(dbDict)
                    status = self.__send2PayTm('UPDATE_REC', recDict)
                    dbDict['ACK'] = 'ACK' if status else 'NACK'
                    self.__persistenceInv.updateDb(dbDict, [['MKT_SYMBOL', dbDict['MKT_SYMBOL']], ['STRATEGY', dbDict['STRATEGY']], ['REC_DATE', dbDict['REC_DATE']]])


    def __transitionRec(self, dbDict, newRec):
        status = False
        if newRec == 'CLOSE' and dbDict['REC_STATUS'] != 'CLOSE':
            status = True
            dbDict['REC_STATUS'] = newRec
        return status, dbDict


    def __closeLeverageRecsNotVisible(self):
        strategiesToCheck = ['MARGIN', 'OPTIONS']
        for strategyToCheck in strategiesToCheck:
            if strategyToCheck == 'MARGIN':
                persistence = self.__persistenceIntraDay
            elif strategyToCheck == 'OPTIONS':
                persistence = self.__persistenceFnO

            # Find all strategyToCheck (MARGIN|OPTIONS|FUTURE) recommendations in DB that are not closed
            dbDicts = persistence.getDb([['STRATEGY', strategyToCheck], ['REC_STATUS', '!CLOSE']])

            # If they are not found in the recommendations on the web page --> close them 
            for dbDict in dbDicts:
                visible = self.__iciciDirect.isVisible(dbDict['SOURCE'], dbDict['ICICI_SYMBOL'], dbDict['STRATEGY'], dbDict['BUY_SELL'])

                # Close the recommendation that was not found
                if not visible:
                    dbDict['REC_STATUS'] = 'CLOSE'
                    dbDict['VISIBLE'] = 'HIDDEN'
                    recDict = self.__iciciDirect.prepareRecDict(dbDict)
                    status = self.__send2PayTm('UPDATE_REC', recDict)
                    dbDict['ACK'] = 'ACK' if status else 'NACK'
                    persistence.updateDb(dbDict, [['MKT_SYMBOL', dbDict['MKT_SYMBOL']], ['STRATEGY', dbDict['STRATEGY']], ['REC_DATE', dbDict['REC_DATE']], ['REC_TIME', dbDict['REC_TIME']], ['REC_STATUS', 'OPEN']])


    def __updateNonLeverageRecStatus(self, recDict):
        recDict['VISIBLE'] = 'VISIBLE'
        persistence = self.__persistenceInv
        _, invPeriod, expDate = self.__computeExpDate(recDict, dbDict)

        if invPeriod != dbDict['INV_PERIOD'] or expDate != dbDict['EXP_DATE']:
            dbDict['INV_PERIOD'] = invPeriod
            dbDict['EXP_DATE'] = expDate
            hasChanged = True

        # Find open recommendations matching the condition in DB
        self.__logger.debug("updateRecStatus: Finding in DB nseSym=%s, strategy=%s, date=%s, time=%s, recStatus=%s", 
                            recDict['MKT_SYMBOL'], recDict['STRATEGY'], recDict['REC_DATE'], recDict['REC_TIME'], 'None')
        isInDb, dbDict = persistence.isInDb([['MKT_SYMBOL', recDict['MKT_SYMBOL']], ['STRATEGY', recDict['STRATEGY']], ['REC_DATE', recDict['REC_DATE']]])
        self.__logger.debug("Find results: status = %s & dbDict = %s", isInDb, dbDict)

        # If no recommendation found in DB and if the current recommendation is not close, then
        # Insert the recommendation in DB
        if not isInDb:
            if(recDict['STATUS'] != 'CLOSE'):
                recDict = self.__paytm.prepareRecDict(recDict)
                status = self.__send2PayTm('NEW_REC', recDict)
                recDict['ACK'] = 'ACK' if status else 'NACK'
                res = persistence.insertDb(recDict, [['MKT_SYMBOL', recDict['MKT_SYMBOL']], ['STRATEGY', recDict['STRATEGY']], ['REC_DATE', recDict['REC_DATE']], ['REC_TIME', recDict['REC_TIME']]])
                self.__logger.info('New Recommendation %s', recDict)
            else:
                recDict['ACK'] = 'ACK'
                res = persistence.insertDb(recDict, [['MKT_SYMBOL', recDict['MKT_SYMBOL']], ['STRATEGY', recDict['STRATEGY']], ['REC_DATE', recDict['REC_DATE']], ['REC_TIME', recDict['REC_TIME']]])
                self.__logger.info("Recommendation for %s is new (i.e. not in DB) but is already closed %s", recDict['MKT_SYMBOL'], recDict)
        elif isInDb:
                # If the recommendation has changed then
                isChange, dbDict = self.__transitionRec(dbDict, recDict['STATUS'])
                if isChange:
                    recDict = self.__paytm.prepareRecDict(dbDict)
                    status = self.__send2PayTm('UPDATE_REC', recDict)
                    dbDict['ACK'] = 'ACK' if status else 'NACK'
                    persistence.updateDb(dbDict, [['MKT_SYMBOL', dbDict['MKT_SYMBOL']], ['STRATEGY', dbDict['STRATEGY']], ['REC_DATE', dbDict['REC_DATE']], ['REC_TIME', dbDict['REC_TIME']]])
                #else: Nothing to be done


    def __updateMismatchedVisibilityNonLeverageRecs(self):
        # Find all strategyToCheck (MARGIN|OPTIONS|FUTURE) recommendations in DB that are not closed
        dbDicts = self.__persistenceInv.getDb([['REC_STATUS', '!CLOSE']])

        # If they are not found in the recommendations on the web page --> close them 
        for dbDict in dbDicts:
            visible = self.__iciciDirect.isVisible(dbDict['SOURCE'], dbDict['ICICI_SYMBOL'], dbDict['STRATEGY'], dbDict['BUY_SELL'])

            # Close the recommendation that was not found
            if visible and (dbDict['VISIBLE'] != 'VISIBLE'):
                dbDict['VISIBLE'] = 'VISIBLE'
                recDict = self.__iciciDirect.prepareRecDict(dbDict)
                status = self.__send2PayTm('UPDATE_REC', recDict)
                dbDict['ACK'] = 'ACK' if status else 'NACK'
                self.__persistenceInv.updateDb(dbDict, [['MKT_SYMBOL', dbDict['MKT_SYMBOL']], ['STRATEGY', dbDict['STRATEGY']], ['REC_DATE', dbDict['REC_DATE']], ['REC_TIME', dbDict['REC_TIME']], ['REC_STATUS', 'OPEN']])
            elif not visible and (dbDict['VISIBLE'] == 'VISIBLE'):
                dbDict['VISIBLE'] = 'VISIBLE'
                recDict = self.__iciciDirect.prepareRecDict(dbDict)
                status = self.__send2PayTm('UPDATE_REC', recDict)
                dbDict['ACK'] = 'ACK' if status else 'NACK'
                self.__persistenceInv.updateDb(dbDict, [['MKT_SYMBOL', dbDict['MKT_SYMBOL']], ['STRATEGY', dbDict['STRATEGY']], ['REC_DATE', dbDict['REC_DATE']], ['REC_TIME', dbDict['REC_TIME']], ['REC_STATUS', 'OPEN']])


    def __sendNonAckedRecsFromDb(self):
        # Find open recommendations matching the condition in DB
        self.__logger.debug("__sendNonAckedRecs: Finding in DB ACK=False")
        persistence = self.__persistenceInv

        dbDicts = persistence.getDb([['ACK', '!ACK']])
        self.__logger.debug("Find results: dbDict = %s", dbDicts)

        for dbDict in dbDicts:
            recDict = self.__paytm.prepareRecDict(dbDict)
            status = self.__send2PayTm('UPDATE_REC', recDict)
            dbDict['ACK'] = 'ACK' if status else 'NACK'
            persistence.updateDb(dbDict, [['MKT_SYMBOL', dbDict['MKT_SYMBOL']], ['STRATEGY', dbDict['STRATEGY']], ['REC_DATE', dbDict['REC_DATE']], ['REC_TIME', dbDict['REC_TIME']]])


    def runPeriodicChecks(self, marketOpen, marketCloseMinusDelta):
        # Send all recommendations in DB that haven't be ACK'ed
        self.__sendNonAckedRecsFromDb()

        # Refresh webpage and find new ideas        
        self.__paytm.refreshIdeas()
        self.__paytm.scrapeIdeas()
        for invRecDict in self.__paytm.getNextPaytmTblRow():
                self.__updateNonLeverageRecStatus(invRecDict)
        
        """ 
        if marketCloseMinusDelta:
            #self.__updateMismatchedVisibilityNonLeverageRecs()
        """

    def openPaytmSession(self):
        self.__paytm.browsePaytm()


    def openBreezeSession(self, on_ticks):
        self.__iciciDirect.openBreezeSession(on_ticks)


def breezeTicks(ticks):
    print(ticks)
    print(datetime.datetime.now())
    #recDict = self.__iciciDirect.getRecDictFromTick(ticks)
    #self.__updateLeverageRecStatus(recDict)


if __name__ == '__main__':
    trade = app('./paytm.ini')
    trade.openPaytmSession()
    while True:
        trade.runPeriodicChecks(True, True)
        time.sleep(5)

    """
    marketClose = False
    while not marketClose:
        marketOpen = datetime.datetime.now() >= datetime.datetime.now().replace(hour=9, minute=15) 
        marketClose = datetime.datetime.now() >= datetime.datetime.now().replace(hour=15, minute=30)
        marketClose = False
        marketCloseMinusDelta = datetime.datetime.now() >= datetime.datetime.now().replace(hour=15, minute=20)
        trade.runPeriodicChecks(marketOpen and not marketClose, marketCloseMinusDelta)
        if not marketOpen:
            time.sleep(15)
    """