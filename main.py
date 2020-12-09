import pandas as pd
import numpy
class DataHandler:
    def __init__(self):
        self.df_lf = None
        self.df_pa = None
        self.df_res = None
    def get_data(self):
        self.df_lf = pd.read_csv("../csv/listings_final.csv",sep=';',index_col=0)
        self.df_pa = pd.read_csv("../csv/price_availability.csv",sep=';')

    def group_data(self):
        self.df_res = pd.merge(self.df_lf,self.df_pa.groupby('listing_id')['local_price'].mean('local_price'),how='inner', on='listing_id')

    def get_process_data(self):
        self.get_data()
        self.group_data()