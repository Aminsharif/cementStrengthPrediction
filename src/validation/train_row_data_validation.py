import sys
from src.logger import logging
from src.exception import CustomException
import json
import os
import shutil
from datetime import datetime
from os import listdir
import re
import pandas as pd

class Row_Data_Validation:

    
    """
        This class shall be used for handling all the validation done on the Raw Training Data!!.

        Written By: Amin Sharif
        Version: 1.0
        Revisions: None

    """
    
    def __self__(self, path):
        self.Batch_Directory = path
        self.schema_path = 'schema_training.json'


    def valuesFromSchema(self):
        """
            Method Name: valuesFromSchema
            Description: This method extracts all the relevant information from the pre-defined "Schema" file.
            Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
            On Failure: Raise ValueError,KeyError,Exception

            Written By: Amin sharif
            Version: 1.0
            Revisions: None
                """
        try:
            with open(self.schema_path, 'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            logging.info(message)



        except ValueError:
            logging.info("ValueError:Value not found inside schema_training.json")
            raise ValueError

        except KeyError:
            logging.info( "KeyError:Key value error incorrect key passed")
            raise KeyError

        except Exception as e:
            logging.info( str(e))
            raise CustomException(e, sys)

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns       
    

    def manualRegexCreation(self):
        """
            Method Name: manualRegexCreation
            Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                    This Regex is used to validate the filename of the training data.
            Output: Regex pattern
            On Failure: None

            Written By: amin Sharif
            Version: 1.0
            Revisions: None

        """
        regex = "['cement_strength']+['\_'']+[\d_]+[\d]+\.csv"
        return regex
    
    
    def createDirectoryForGoodBadRawData(self):

        """
            Method Name: createDirectoryForGoodBadRawData
            Description: This method creates directories to store the Good Data and Bad Data
                after validating the training data.

            Output: None
            On Failure: OSError

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None

            """

        try:
            path = os.path.join("../../articafts/Training_Raw_files_validated/", "Good_Raw/")
            path = os.path.join('')

            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("../../articafts/Training_Raw_files_validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            logging.info("Error while creating Directory %s:" % ex)
            raise OSError
        
    def deleteExistingGoodDataTrainingFolder(self):

        """
            Method Name: deleteExistingGoodDataTrainingFolder
            Description: This method deletes the directory made  to store the Good Data
            after loading the data in the table. Once the good files are
            loaded in the DB,deleting the directory ensures space optimization.
            Output: None
            On Failure: OSError

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None

         """

        try:
            path = '../../articafts/Training_Raw_files_validated/'
            # if os.path.isdir("ids/" + userName):
            # if os.path.isdir(path + 'Bad_Raw/'):
            #     shutil.rmtree(path + 'Bad_Raw/')
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                logging.info("GoodRaw directory deleted successfully!!!")
        except OSError as s:
            logging.info("Error while Deleting Directory : %s" %s)
            raise OSError
        
    def deleteExistingBadDataTrainingFolder(self):

        """
            Method Name: deleteExistingBadDataTrainingFolder
            Description: This method deletes the directory made to store the bad Data.
            Output: None
            On Failure: OSError

            Written By: iNeuron Intelligence
            Version: 1.0
            Revisions: None

        """

        try:
            path = '../../articafts/Training_Raw_files_validated/'
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                logging.info("BadRaw directory deleted before starting validation!!!")
        except OSError as s:
            logging.info("Error while Deleting Directory : %s" %s)
            raise OSError

    
    def moveBadFilesToArchiveBad(self):

        """
            Method Name: moveBadFilesToArchiveBad
            Description: This method deletes the directory made  to store the Bad Data
                after moving the data in an archive folder. We archive the bad
                files to send them back to the client for invalid data issue.
            Output: None
            On Failure: OSError

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None

        """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:

            source = '../../articafts/Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "../../articafts/TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = '../../articafts/TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                
                logging.info("Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                logging.info("Bad Raw Data Folder Deleted successfully!!")
        except Exception as e:
            logging.info("Error while moving bad files to archive:: %s" % e)
            raise CustomException(e, sys)
        
    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: Amin Sharif
                    Version: 1.0
                    Revisions: None

                """

        #pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()

        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            # create new directories
            self.createDirectoryForGoodBadRawData()
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[2]) == LengthOfDateStampInFile:
                        if len(splitAtDot[3]) == LengthOfTimeStampInFile:
                            shutil.copy("../../articafts/Training_Batch_Files/" + filename, "../../articafts/Training_Raw_files_validated/Good_Raw")
                            logging.info("Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            shutil.copy("../../articafts/Training_Batch_Files/" + filename, "../../articafts/Training_Raw_files_validated/Bad_Raw")
                            logging.info("Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                    else:
                        shutil.copy("../../articafts/Training_Batch_Files/" + filename, "../../articafts/Training_Raw_files_validated/Bad_Raw")
                        logging.info("Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    shutil.copy("../../articafts/Training_Batch_Files/" + filename, "../../articafts/Training_Raw_files_validated/Bad_Raw")
                    logging.info("Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)


        except Exception as e:
           
            logging.info("Error occured while validating FileName %s" % e)
            raise CustomException(e, sys)
        
    
    def validateColumnLength(self,NumberofColumns):
        """
                          Method Name: validateColumnLength
                          Description: This function validates the number of columns in the csv files.
                                       It is should be same as given in the schema file.
                                       If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                       If the column number matches, file is kept in Good Raw Data for processing.
                                      The csv file is missing the first column name, this function changes the missing name to "Wafer".
                          Output: None
                          On Failure: Exception

                        Written By: Amin Sharif
                          Version: 1.0
                          Revisions: None

                      """
        try:
            f = open("../../articafts/Training_Logs/columnValidationLog.txt", 'a+')
            for file in listdir('../../articafts/Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("../../articafts/Training_Raw_files_validated/Good_Raw/" + file)
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    shutil.move("../../articafts/Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    logging.info("Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            logging.info("Column Length Validation Completed!!")
        except OSError:
            logging.info("Error Occured while moving the file :: %s" % OSError)
            raise OSError
        except Exception as e:
            logging.info("Error Occured:: %s" % e)
            raise CustomException(e, sys)
    
    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: Amin Sharif
                                  Version: 1.0
                                  Revisions: None

                              """
        try:
            logging.info("Missing Values Validation Started!!")

            for file in listdir('../../articafts/Training_Raw_files_validated/Good_Raw/'):
                csv = pd.read_csv("../../articafts/Training_Raw_files_validated/Good_Raw/" + file)
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        shutil.move("../../articafts/Training_Raw_files_validated/Good_Raw/" + file,
                                    "../../articafts/Training_Raw_files_validated/Bad_Raw")
                        logging.info("Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv.to_csv("../../articafts/Training_Raw_files_validated/Good_Raw/" + file, index=None, header=True)
        except OSError:
            logging.info("Error Occured while moving the file :: %s" % OSError)
            raise OSError
        except Exception as e:
            
            logging.info("Error Occured:: %s" % e)
            raise CustomException(e, sys)