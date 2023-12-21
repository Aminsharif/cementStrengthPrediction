import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler
from src.logger import logging
from src.exception import CustomException

import sys
import os
class Preprocessor:
    """
        This class shall  be used to clean and transform the data before training.

        Written By: Amin sharif
        Version: 1.0
        Revisions: None

     """
    def __init__(self):
        pass

    def remove_columns(self,data,columns):
        """
                Method Name: remove_columns
                Description: This method removes the given columns from a pandas dataframe.
                Output: A pandas DataFrame after removing the specified columns.
                On Failure: Raise Exception

                Written By: Amin shairf
                Version: 1.0
                Revisions: None

        """
        logging.info( 'Entered the remove_columns method of the Preprocessor class')
        self.data=data
        self.columns=columns
        try:
            self.useful_data=self.data.drop(labels=self.columns, axis=1) # drop the labels specified in the columns
            logging.info('Column removal Successful.Exited the remove_columns method of the Preprocessor class')
            return self.useful_data
        except Exception as e:
            logging.info('Exception occured in remove_columns method of the Preprocessor class. Exception message:  '+str(e))
            logging.info('Column removal Unsuccessful. Exited the remove_columns method of the Preprocessor class')
            raise CustomException(e, sys)
        
    def separate_label_feature(self, data, label_column_name):
        """
                        Method Name: separate_label_feature
                        Description: This method separates the features and a Label Coulmns.
                        Output: Returns two separate Dataframes, one containing features and the other containing Labels .
                        On Failure: Raise Exception

                        Written By: Amin Sharif
                        Version: 1.0
                        Revisions: None

                """
        logging.info( 'Entered the separate_label_feature method of the Preprocessor class')
        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            logging.info('Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X,self.Y
        except Exception as e:
            logging.info('Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            logging.info( 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise CustomException(e, sys)
        
    
    def dropUnnecessaryColumns(self,data,columnNameList):
        """
            Method Name: is_null_present
            Description: This method drops the unwanted columns as discussed in EDA section.

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None

        """
        data = data.drop(columnNameList,axis=1)
        return data
    
    def replaceInvalidValuesWithNull(self,data):

        """
            Method Name: is_null_present
            Description: This method replaces invalid values i.e. '?' with null, as discussed in EDA.

            Written By: iNeuron Intelligence
            Version: 1.0
            Revisions: None

        """

        for column in data.columns:
            count = data[column][data[column] == '?'].count()
            if count != 0:
                data[column] = data[column].replace('?', np.nan)
        return data
    
    def is_null_present(self,data):
        """
            Method Name: is_null_present
            Description: This method checks whether there are null values present in the pandas Dataframe or not.
            Output: Returns True if null values are present in the DataFrame, False if they are not present and
            returns the list of columns for which null values are present.
            On Failure: Raise Exception

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None

        """
        logging.info( 'Entered the is_null_present method of the Preprocessor class')
        self.null_present = False
        self.cols_with_missing_values=[]
        self.cols = data.columns
        try:
            self.null_counts=data.isna().sum() # check for the count of null values per column
            for i in range(len(self.null_counts)):
                if self.null_counts[i]>0:
                    self.null_present=True
                    self.cols_with_missing_values.append(self.cols[i])
            if(self.null_present): # write the logs to see which columns have null values
                self.dataframe_with_null = pd.DataFrame()
                self.dataframe_with_null['columns'] = data.columns
                self.dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
                self.dataframe_with_null.to_csv('../../articafts/preprocessing_data/null_values.csv') # storing the null column information to file
            logging.info('Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class')
            return self.null_present, self.cols_with_missing_values
        except Exception as e:
            logging.info('Exception occured in is_null_present method of the Preprocessor class. Exception message:  ' + str(e))
            logging.info('Finding missing values failed. Exited the is_null_present method of the Preprocessor class')
            raise CustomException(e, sys)
        
    def encodeCategoricalValues(self,data):
        """
            Method Name: encodeCategoricalValues
            Description: This method encodes all the categorical values in the training set.
            Output: A Dataframe which has all the categorical values encoded.
            On Failure: Raise Exception

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None
        """
        data["class"] = data["class"].map({'p': 1, 'e': 2})

        for column in data.drop(['class'],axis=1).columns:
            data = pd.get_dummies(data, columns=[column])

        return data

    def encodeCategoricalValuesPrediction(self,data):
        """
            Method Name: encodeCategoricalValuesPrediction
            Description: This method encodes all the categorical values in the prediction set.
            Output: A Dataframe which has all the categorical values encoded.
            On Failure: Raise Exception

            Written By: Amin Sharif
            Version: 1.0
            Revisions: None
        """

        for column in data.columns:
            data = pd.get_dummies(data, columns=[column])

        return data
    
    def standardScalingData(self,X):

        scalar = StandardScaler()
        X_scaled = scalar.fit_transform(X)

        return X_scaled

    def logTransformation(self,X):

        for column in X.columns:
            X[column] += 1
            X[column] = np.log(X[column])

        return X
    
    def impute_missing_values(self, data):
        """
            Method Name: impute_missing_values
            Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
            Output: A Dataframe which has all the missing values imputed.
            On Failure: Raise Exception

            Written By: iNeuron Intelligence
            Version: 1.0
            Revisions: None
        """
        logging.info( 'Entered the impute_missing_values method of the Preprocessor class')
        self.data= data
        try:
            imputer=KNNImputer(n_neighbors=3, weights='uniform',missing_values=np.nan)
            self.new_array=imputer.fit_transform(self.data) # impute the missing values
            # convert the nd-array returned in the step above to a Dataframe
            self.new_data=pd.DataFrame(data=(self.new_array), columns=self.data.columns)
            logging.info( 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class')
            return self.new_data
        except Exception as e:
            logging.info('Exception occured in impute_missing_values method of the Preprocessor class. Exception message:  ' + str(e))
            logging.info('Imputing missing values failed. Exited the impute_missing_values method of the Preprocessor class')
            raise CustomException(e, sys)
        
    
    def get_columns_with_zero_std_deviation(self,data):
        """
            Method Name: get_columns_with_zero_std_deviation
            Description: This method finds out the columns which have a standard deviation of zero.
            Output: List of the columns with standard deviation of zero
            On Failure: Raise Exception

            Written By: iNeuron Intelligence
            Version: 1.0
            Revisions: None
        """
        logging.info( 'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class')
        self.columns=data.columns
        self.data_n = data.describe()
        self.col_to_drop=[]
        try:
            for x in self.columns:
                if (self.data_n[x]['std'] == 0): # check if standard deviation is zero
                    self.col_to_drop.append(x)  # prepare the list of columns with standard deviation zero
            logging.info( 'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            return self.col_to_drop

        except Exception as e:
            logging.info('Exception occured in get_columns_with_zero_std_deviation method of the Preprocessor class. Exception message:  ' + str(e))
            logging.info( 'Column search for Standard Deviation of Zero Failed. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class')
            raise CustomException(e, sys)