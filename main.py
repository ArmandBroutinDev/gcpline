import pandas as pd
import numpy
class DataHandler:
    def __init__(self):
        print("intialisation")
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