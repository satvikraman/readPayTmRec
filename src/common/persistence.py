import logging
import os
import re
import configparser
from tinydb import TinyDB, Query, where
from tinydb.operations import delete

class persistence:
    def __init__(self, configFile, db, lock=None):
        if(os.path.isfile(configFile)):
            self.__config = configparser.ConfigParser()
            self.__config.read(configFile)

            if(self.__config['DATABASE']['LOG_LEVEL'] == 'DEBUG'):
                level = logging.DEBUG
            elif(self.__config['DATABASE']['LOG_LEVEL'] == 'INFO'):
                level = logging.INFO
            elif(self.__config['DATABASE']['LOG_LEVEL'] == 'WARNING'):
                level = logging.WARNING
            elif(self.__config['DATABASE']['LOG_LEVEL'] == 'ERROR'):
                level = logging.ERROR
            elif(self.__config['DATABASE']['LOG_LEVEL'] == 'CRITICAL'):
                level = logging.CRITICAL
            self.__logger = logging.getLogger(__name__)
            self.__logger.setLevel(level)
        
            self.__lock = lock
            self.__db = TinyDB(db)
            self.__query = Query()


    def __acquireLock(self):
        if(self.__lock != None):
            self.__lock.acquire()


    def __releaseLock(self):
        if(self.__lock != None):
            self.__lock.release()


    def __formSubQuery(self, keyword, val):
        inverse = False
        if '!' in val:
            inverse = True
            val = re.sub(r'!', '', val)

        if '|' in val:
            vals = val.split('|')
            query = (where(keyword) == vals[0])
            for item in vals[1:]:
                query = query | (where(keyword) == item)
        elif '&&' in val:
            vals = val.split('&&')
            query = (where(keyword) == vals[0])
            for item in vals[1:]:
                query = query & (where(keyword) == item)
        else:
            query = (where(keyword) == val)
        
        query = (~(query)) if inverse else (query)
        return query


    def __formQuery(self, queryParamVals):
        query = self.__query.noop()
        for queryParamVal in queryParamVals:
            query = query & self.__formSubQuery(queryParamVal[0], queryParamVal[1])
        return query


    def getDb(self, queryParamVals):
        dictArr = [{}]
        query = self.__formQuery(queryParamVals)
        if(query != None):
            self.__acquireLock()
            dictArr = self.__db.search(query)
            self.__releaseLock()
        return dictArr


    def insertDb(self, dict, queryParamVals):
        status = False
        found, retDict = self.isInDb(queryParamVals)
        if(not found and dict):
            self.__acquireLock()
            res = self.__db.insert(dict)
            self.__releaseLock()
            if res > 0:
                status = True
            else:
                self.__logger.critical("Unable to insert record in DB: %s", dict)
        else:
            self.__logger.error("Record already in DB. Can't insert: %s. \nFound: %s", dict, retDict)
        return status
        

    def updateDb(self, dict, queryParamVals):
        status = False
        query = self.__formQuery(queryParamVals)
        if(query != None):
            self.__acquireLock()
            res = self.__db.update(dict, query)
            self.__releaseLock()
            status = True if len(res) > 0 else False
        return status
    

    def removeKeyFromDb(self, key, queryParamVals):
        query = self.__formQuery(queryParamVals)
        self.__acquireLock()
        res = self.__db.update(delete(key), query)
        self.__releaseLock()


    def removeFromDb(self, queryParamVals):
        query = self.__formQuery(queryParamVals)
        if(query != None):
            self.__acquireLock()
            self.__db.remove(query)
            self.__releaseLock()
    

    def isInDb(self, queryParamVals):
        status = False
        dictsArr = self.getDb(queryParamVals)
        # We expect only a single record for a given symbol in a given strategy
        if(len(dictsArr) == 1):
            status = True
            retDict = dictsArr[0]
        else:
            retDict = {}
        return status, retDict


    def removeAll(self):
        self.__acquireLock()
        self.__db.truncate()
        self.__releaseLock()
