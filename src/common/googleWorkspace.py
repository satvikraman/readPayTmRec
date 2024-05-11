import logging
import os

import google.auth
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload


class googleWorkspace():
    def __init__(self, spreadsheetID=None, sheetName=None):
        self.__spreadsheetID = spreadsheetID
        self.__sheetName = sheetName
        self.__sheetcreds = None
        self.__drivecreds = None
        self.__logger = logging.getLogger(__name__)
        self.__logger.setLevel(logging.INFO)

        return


    def authorize(self):
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists('token.json'):
            self.__sheetcreds = Credentials.from_authorized_user_file('token.json', SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not self.__sheetcreds or not self.__sheetcreds.valid:
            if self.__sheetcreds and self.__sheetcreds.expired and self.__sheetcreds.refresh_token:
                self.__sheetcreds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                self.__sheetcreds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.json', 'w') as token:
                token.write(self.__sheetcreds.to_json())


    def buildSheets(self):
        self.__sheet = build('sheets', 'v4', credentials=self.__sheetcreds)


    def buildDrive(self):
        self.__drive = build('drive', 'v3', credentials=self.__sheetcreds)


    def writeToCell(self, colRowFrom, colRowTo, values):
        try:
            status = False
            writeRange = self.__sheetName+'!'+colRowFrom+':'+colRowTo
            body = {
                'values': values
            }
            result = self.__sheet.spreadsheets().values().update(spreadsheetId=self.__spreadsheetID,
                                                                 range=writeRange, 
                                                                 valueInputOption="USER_ENTERED", 
                                                                 body=body).execute()
            if result.get('updatedCells') == 1:
                status = True
        except HttpError as error:
            self.__logger.error("Error: %s", error)

        return status


    def readFromCell(self, colRowFrom, colRowTo):
        try:
            status = True
            readRange = self.__sheetName+'!'+colRowFrom+':'+colRowTo
            result = self.__sheet.spreadsheets().values().get(spreadsheetId=self.__spreadsheetID,
                                                              range=readRange).execute()
            values = result.get('values', [])
            if not values:
                self.__logger.error("No data found")
                status = False
        except Exception as error:
            status = False
            self.__logger.error("Error: %s", error)

        return status, values

    
    def uploadMediaFile(self, filePath, mimeTyp):
        try:
            status = True
            file_metadata = {"name": filePath}
            media = MediaFileUpload(filePath, mimetype=mimeTyp, resumable=True)
            file = (
                self.__drive.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            id = file.get("id")
            self.__logger.info("File ID: %s", file.get("id"))

        except HttpError as error:
            status = False
            self.__logger.error("Error: %s", error)

        return status, id


    def deleteMediaFile(self, id):
        try:
            status = True
            file = (
                self.__drive.files()
                .delete(fileId=id)
                .execute()
            )
        except HttpError as error:
            status = False
            self.__logger.error("Error: %s", error)

        return status