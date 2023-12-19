from datetime import datetime
from src.logger import logging
from src.exception import CustomException
import sys
from train_row_data_validation import Row_Data_Validation
from db.db_operation import dBOperation



class train_validation:
    def __self__(self, path):
        self.raw_data = Row_Data_Validation(path)
        self.dBOperation = dBOperation()

    def train_validation(self):
        try:
            logging.info( 'Start of Validation on files for prediction!!')
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            logging.info( "Raw Data Validation Complete!!")

            logging.info("Creating Training_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dBOperation.createTableDb('Training', column_names)
            logging.info( "Table creation Completed!!")
            logging.info( "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.dBOperation.insertIntoTableGoodData('Training')
            logging.info( "Insertion in Table completed!!!")
            logging.info( "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            logging.info( "Good_Data folder deleted!!!")
            logging.info( "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            logging.info( "Bad files moved to archive!! Bad folder Deleted!!")
            logging.info( "Validation Operation completed!!")
            logging.info( "Extracting csv file from table")
            # export data in table to csvfile
            self.dBOperation.selectingDatafromtableintocsv('Training')

        except Exception as e:
            raise CustomException(e, sys)

