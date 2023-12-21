import pickle
import os
import shutil
from src.logger import logging
from src.exception import CustomException
import sys

class File_Operation:
    """
        This class shall be used to save the model after training
        and load the saved model for prediction.

         Written By: Amin Sharif
        Version: 1.0
        Revisions: None

    """
    def __init__(self):
        self.model_directory='../../articafts/models/'

    def save_model(self,model,filename):
        """
            Method Name: save_model
            Description: Save the model file to directory
            Outcome: File gets saved
            On Failure: Raise Exception

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None
        """
        logging.info('Entered the save_model method of the File_Operation class')
        try:
            path = os.path.join(self.model_directory,filename) #create seperate directory for each cluster
            if os.path.isdir(path): #remove previously existing models for each clusters
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path) #
            with open(path +'/' + filename+'.sav',
                      'wb') as f:
                pickle.dump(model, f) # save the model to file
            logging.info('Model File '+filename+' saved. Exited the save_model method of the Model_Finder class')

            return 'success'
        except Exception as e:
            logging.info('Exception occured in save_model method of the Model_Finder class. Exception message:  ' + str(e))
            logging.info('Model File '+filename+' could not be saved. Exited the save_model method of the Model_Finder class')
            raise CustomException(e, sys)

    def load_model(self,filename):
        """
            Method Name: load_model
            Description: load the model file to memory
            Output: The Model file loaded in memory
            On Failure: Raise Exception

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None
        """
        logging.info('Entered the load_model method of the File_Operation class')
        try:
            with open(self.model_directory + filename + '/' + filename + '.sav',
                      'rb') as f:
                logging.info('Model File ' + filename + ' loaded. Exited the load_model method of the Model_Finder class')
                return pickle.load(f)
        except Exception as e:
            logging.info('Exception occured in load_model method of the Model_Finder class. Exception message:  ' + str(e))
            logging.info('Model File ' + filename + ' could not be saved. Exited the load_model method of the Model_Finder class')
            raise CustomException(e, sys)

    def find_correct_model_file(self,cluster_number):
        """
            Method Name: find_correct_model_file
            Description: Select the correct model based on cluster number
            Output: The Model file
            On Failure: Raise Exception

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None
        """
        logging.info('Entered the find_correct_model_file method of the File_Operation class')
        try:
            self.cluster_number= cluster_number
            self.folder_name=self.model_directory
            self.list_of_model_files = []
            self.list_of_files = os.listdir(self.folder_name)
            for self.file in self.list_of_files:
                try:
                    if (self.file.index(str( self.cluster_number))!=-1):
                        self.model_name=self.file
                except:
                    continue
            self.model_name=self.model_name.split('.')[0]
            logging.info('Exited the find_correct_model_file method of the Model_Finder class.')
            return self.model_name
        except Exception as e:
            logging.info('Exception occured in find_correct_model_file method of the Model_Finder class. Exception message:  ' + str(e))
            logging.info('Exited the find_correct_model_file method of the Model_Finder class with Failure')
            raise CustomException(e, sys)