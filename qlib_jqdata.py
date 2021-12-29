import jqdatasdk
import os
from tqdm import tqdm

class qlib_jqdata():
    default_fields = ['open','low','high','close','volume','factor']
    def __init__(self, stock_list, fields=default_fields, path='~/.qlib/csv_data/my_data', start_date="2019-11-21", end_date="2019-12-01", fq='post') -> None:
        username = input('Please input your username (phone number):')
        password = input('Input your pass word:')
        jqdatasdk.auth(username, password)
        query_count = jqdatasdk.get_query_count()
        print('spare / total: {} / {}'.format(query_count['spare'], query_count['total']))
        self.stock_list = stock_list
        self.stock_list_jq = jqdatasdk.normalize_code(stock_list)
        self.fields = fields
        self.path = path
        self.start_date, self.end_date = start_date, end_date
        self.fq = fq
        if not os.path.exists(path):
            os.makedirs(path)
        

    def get_data(self, conv2list=True):
        df = jqdatasdk.get_price(self.stock_list_jq, start_date=self.start_date, end_date=self.end_date, 
                                 fields=self.fields, fq=self.fq)
        df.rename(columns={'code':'symbol', 'time':'date'}, inplace=True)  # 重命名
        if conv2list:
            out = []
            for k, ori_k in zip(self.stock_list_jq, self.stock_list):
                d_k = self.df[self.df.loc[:,'code']==k]
                d_k.loc[:, 'code'] = ori_k
                out.append(d_k)
            return df
        else:
            return df
    
    def save_data(self, df_list):
        for df in tqdm(df_list, desc='Saving to .csv'):
            stock = df.loc[0, 'symbol']
            df.to_csv(stock + ".csv", index=False)
        