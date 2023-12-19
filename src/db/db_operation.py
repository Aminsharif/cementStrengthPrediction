import shutil
import sqlite3
from os import listdir
import os
import csv
import sys
from src.logger import logging
from src.exception import CustomException



class dBOperation:
    """
          This class shall be used for handling all the SQL operations.

          Written By: Amin Sharif
          Version: 1.0
          Revisions: None

          """
    def __init__(self):
        self.path = '../../articafts/Prediction_Database/'
        self.badFilePath = "../../articafts/Prediction_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "../../articafts/Prediction_Raw_Files_Validated/Good_Raw"
    
    def dataBaseConnection(self,DatabaseName):

        """
                        Method Name: dataBaseConnection
                        Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
                        Output: Connection to the DB
                        On Failure: Raise ConnectionError

                        Written By: Amin Sharif
                        Version: 1.0
                        Revisions: None

                        """
        try:
            conn = sqlite3.connect(self.path+DatabaseName+'.db')
            logging.info("Opened %s database successfully" % DatabaseName)
        except ConnectionError:
            
            logging.info("Error while connecting to database: %s" %ConnectionError)
            raise ConnectionError
        return conn
    def createTableDb(self,DatabaseName,column_names):

        """
           Method Name: createTableDb
           Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
           Output: None
           On Failure: Raise Exception

            Written By: iNeuron Intelligence
           Version: 1.0
           Revisions: None

        """
        try:
            conn = self.dataBaseConnection(DatabaseName)
            conn.execute('DROP TABLE IF EXISTS Good_Raw_Data;')

            for key in column_names.keys():
                type = column_names[key]

                # we will remove the column of string datatype before loading as it is not needed for training
                #in try block we check if the table exists, if yes then add columns to the table
                # else in catch block we create the table
                try:
                    #cur = cur.execute("SELECT name FROM {dbName} WHERE type='table' AND name='Good_Raw_Data'".format(dbName=DatabaseName))
                    conn.execute('ALTER TABLE Good_Raw_Data ADD COLUMN "{column_name}" {dataType}'.format(column_name=key,dataType=type))
                except:
                    conn.execute('CREATE TABLE  Good_Raw_Data ({column_name} {dataType})'.format(column_name=key, dataType=type))

            conn.close()

            logging.info("Tables created successfully!!")
        
            file = open("Prediction_Logs/DataBaseConnectionLog.txt", 'a+')
            logging.info("Closed %s database successfully" % DatabaseName)
            

        except Exception as e:
            logging.info("Error while creating table: %s " % e)
            conn.close()
            logging.info("Closed %s database successfully" % DatabaseName)
            
            raise CustomException(e, sys)
        
    
    def insertIntoTableGoodData(self,Database):

        """
            Method Name: insertIntoTableGoodData
            Description: This method inserts the Good data files from the Good_Raw folder into the
                above created table.
            Output: None
            On Failure: Raise Exception

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None

        """

        conn = self.dataBaseConnection(Database)
        goodFilePath= self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]

        for file in onlyfiles:
            try:

                with open(goodFilePath+'/'+file, "r") as f:
                    next(f)
                    reader = csv.reader(f, delimiter="\n")
                    for line in enumerate(reader):
                        for list_ in (line[1]):
                            try:
                                conn.execute('INSERT INTO Good_Raw_Data values ({values})'.format(values=(list_)))
                                logging.info(" %s: File loaded successfully!!" % file)
                                conn.commit()
                            except Exception as e:
                                raise CustomException(e, sys)

            except Exception as e:
                conn.rollback()
                logging.info("Error while creating table: %s " % e)
                shutil.move(goodFilePath+'/' + file, badFilePath)
                logging.info( "File Moved Successfully %s" % file)
                conn.close()
                raise CustomException(e, sys)

        conn.close()

    
    def selectingDatafromtableintocsv(self,Database):

        """
            Method Name: selectingDatafromtableintocsv
            Description: This method exports the data in GoodData table as a CSV file. in a given location.
                above created .
            Output: None
            On Failure: Amin Sharif

            Written By: iNeuron Intelligence
            Version: 1.0
            Revisions: None

        """

        self.fileFromDb = '../../articafts/Prediction_FileFromDB/'
        self.fileName = 'InputFile.csv'
        try:
            conn = self.dataBaseConnection(Database)
            sqlSelect = "SELECT *  FROM Good_Raw_Data"
            cursor = conn.cursor()

            cursor.execute(sqlSelect)

            results = cursor.fetchall()

            #Get the headers of the csv file
            headers = [i[0] for i in cursor.description]

            #Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Open CSV file for writing.
            csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')

            # Add the headers and data to the CSV file.
            csvFile.writerow(headers)
            csvFile.writerows(results)

            logging.info("File exported successfully!!!")

        except Exception as e:
            logging.info("File exporting failed. Error : %s" %e)
            raise CustomException(e, sys)
