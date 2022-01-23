import jqdatasdk
import os
from tqdm import tqdm

class qlib_jqdata():
    default_fields = ['open','low','high','close','volume','factor']
    def __init__(self, stock_list=None, fields=default_fields, path='D:/qlib/qlib_data/my_data', start_date="2019-11-21", end_date="2019-12-01", fq='post') -> None:
        username = input('Please input your username (phone number):')
        password = input('Input your pass word:')
        jqdatasdk.auth(username, password)
        query_count = jqdatasdk.get_query_count()
        print('spare / total: {} / {}'.format(query_count['spare'], query_count['total']))
        if stock_list:
            self.stock_list = stock_list
            self.stock_list_jq = jqdatasdk.normalize_code(stock_list)
        else:
            self.stock_list = None
            self.stock_list_jq = None
        self.fields = fields
        self.path = path
        self.start_date, self.end_date = start_date, end_date
        self.fq = fq
        if not os.path.exists(path):
            os.makedirs(path)

    def get_all_securities(self, start=0, end=-1):
        df=jqdatasdk.get_all_securities() # 获取所有列表
        # 删除无用列
        del df['display_name']
        del df['name']
        del df['type']
        df['end_date'] = df['end_date'].replace('2200-01-01', self.end_date) # 替换仍在上市股票到今日
        instruments_path = os.path.join(self.path, 'instruments')
        if not os.path.exists(instruments_path):
            os.makedirs(instruments_path)
        df.to_csv(os.path.join(instruments_path, 'all_stock.txt'), sep='\t', header=0)
        self.all_securities = df[start:end]

    def update_all_securities_data_day(self):
        if hasattr(self, 'all_securities'):
            print('Securities have been loaded!')
        else:
            self.get_all_securities()
        with tqdm(total=len(self.all_securities)) as pbar:
            for name, row in tqdm(self.all_securities.iterrows(), desc='当前处理股票数'):
                if os.path.exists(os.path.join(self.path, name + ".csv")):
                    print('skip {}'.format(name))
                    pbar.update(1)
                    continue
                df = self.get_data(stock_name=name, conv2list=False)
                self.save_data(df, symbol=name, is_list=False)
                pbar.update(1)
        


    def get_data(self, stock_name=None, conv2list=True):
        if stock_name:
            df = jqdatasdk.get_price(stock_name, start_date=self.start_date, end_date=self.end_date, 
                                 fields=self.fields, fq=self.fq)
            df.index.name = 'date'
            df.insert(0, 'symbol', stock_name, allow_duplicates=False)
            
        else:
            print('Use stock_list')
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
    
    def save_data(self, df_list, symbol=None, is_list=True):
        if is_list:
            for df in tqdm(df_list, desc='Saving to .csv'):
                stock = df.loc[0, 'symbol']
                df.to_csv(stock + ".csv", index=False)
        else:
            df_list.index.name = 'date'
            df_list.to_csv(os.path.join(self.path, symbol + ".csv"), index=True)