import pandas as pd
from src.logger import logging
from src.exception import CustomException
import sys
import os

class Data_Getter:
    def __init__(self):
        self.traing_file = '../../articafts/Training_FileFromDB/InputFile.csv'

    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

         Written By: Amin sharif
        Version: 1.0
        Revisions: None

        """
        logging.info('Entered the get_data method of the Data_Getter class')
        try:
            self.data= pd.read_csv(self.training_file) # reading the data file
            logging.info('Data Load Successful.Exited the get_data method of the Data_Getter class')
            return self.data
        except Exception as e:
            logging.info('Exception occured in get_data method of the Data_Getter class. Exception message: '+str(e))
            logging.info('Data Load Unsuccessful.Exited the get_data method of the Data_Getter class')
            raise CustomException(e, sys) 