import pandas as pd
import numpy
class DataHandler:
    """
        Getting data from bucket
    """
    def __init__(self):
        """
            Initialising the 3 datasets :
            entry 1
            entry 2
            result
        """
        print("DataHandler intialisation")
        self.df_lf = None
        self.df_pa = None
        self.df_res = None
        print("intialisation done")

    def get_data(self):
        print("loading data from bucket")
        self.df_lf = pd.read_csv("https://storage.googleapis.com/h3-data/listings_final.csv",sep=';')
        self.df_pa = pd.read_csv("https://storage.googleapis.com/h3-data/price_availability.csv",sep=';')
        print("data loaded from bucket")

    def group_data(self):
        print("merging data")
        self.df_res = pd.merge(self.df_lf,self.df_pa.groupby('listing_id')['local_price'].mean('local_price'),how='inner', on='listing_id')
        print("size of the merged data : {} lines, {} columns".format(self.df_res.shape[0],self.df_res.shape[1]))

    def get_process_data(self):
        self.get_data()
        self.group_data()
        print("end of processing")

class FeatureRecipe:

    def __init__(self,df:pd.DataFrame):
        print("FeatureRecipe intialisation")
        self.df = df
        self.cate = []
        self.floa = []
        self.intt = []
        self.drop = []
        print("end of intialisation")

    def separate_variable_types(self) -> None:
        print("separating columns")
        for col in self.df.columns:
            if self.df[col].dtypes == int:
                self.intt.append(self.df[col])
            elif self.df[col].dtypes == float:
                self.floa.append(self.df[col])
            else:
                self.cate.append(self.df[col])
        print ("dataset column size : {} \nnumber of discreet values : {} \nnumber of continuous values : {} \nnumber of others : {} \ntaille total : {}".format(len(self.df.columns),len(self.intt),len(self.floa),len(self.cate),len(self.intt)+len(self.floa)+len(self.cate) ))

    def drop_na_prct(self,threshold : float):
        """
            on appelle la commande et on met un threshold entre 1 et 0 en flottant
            params: threshold : float
        """
        # par rapport a la colonne
        dropped = 0
        print("dropping columns with {} percentage ".format(threshold))
        for col in self.df.columns:
            if self.df[col].isna().sum()/self.df.shape[0] >= threshold:
                self.drop.append( self.df.drop([col], axis='columns', inplace=True) )
                dropped+=1
        print("dropped {} columns".format(dropped))

    def drop_useless_features(self):
        # droper les col vides et doublons de l'index et les colonnes qu'on va considerer inutile
        print("dropping useless columns")
        if 'Unnamed: 0' in self.df.columns:
            self.df.drop(['Unnamed: 0'], axis='columns', inplace=True)
        for col in self.df.columns:
            if self.df[col].isna().sum() == len(self.df):
                self.df.drop([col], axis='columns', inplace=True)
        print("done dropping")

    def drop_duplicate(self):
        # comparer les colonnes et voir si elles sont dupliquées
        print("dropping duplicated rows")
        self.df.drop_duplicates(inplace=True)
        print("duplicated rows dropped")

    #def deal_date_time(self):
    #   pass

    def get_process_data(self,threshold : float):
        self.drop_useless_features()
        self.drop_na_prct(threshold)
        self.drop_duplicate()
        self.separate_variable_types()
        print("end of FeatureRecipe processing")

import sklearn as skn
import matplotlib as plt
from sklearn.model_selection import train_test_split

class FeatureExtractor:
    """
    Feature Extractor class
    """
    def __init__(self, data: pd.DataFrame[], flist: list):
        """
            Input : pandas.DataFrame, feature list to drop
            Output : X_train, X_test, y_train, y_test according to sklearn.model_selection.train_test_split
        """
        self.X_train, self.X_test, self.y_train, self.y_test = None,None,None,None
        self.df = data
        self.flist = flist
    def splitting(size:float,rng:int,X pd.DataFrame[]):
        self.X_train, self.X_test, self.y_train, self.y_test train_test_split(X, y, size, rng)
