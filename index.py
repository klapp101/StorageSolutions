import requests
import pandas as pd
import numpy as np
import xml.etree.ElementTree as ET
import xmltodict
import pyodbc
import pymssql
import sqlalchemy 
from sqlalchemy import create_engine,inspect
import urllib
import json
import warnings
warnings.filterwarnings('ignore')

server = 'server\server'
username = 'username'
password = 'password'
driver = 'ODBC Driver 17 for SQL Server'
database = 'stage'
conn_str = (
r'DRIVER={ODBC Driver 17 for SQL Server};'
r'SERVER=server\server;'
r'DATABASE=db;'
r'Trusted_Connection=yes;' )

engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn_str, fast_executemany=True)
conn = engine.connect()

# https://stackoverflow.com/questions/6473925/sqlalchemy-getting-a-list-of-tables

class StorageSolutionsData:
    def __init__(self):
        pass
    def get_token(self):
        data = {
            'f': 'login',
            'username': 'user',
            'password': 'pw'
        }

        url = 'localhost'
        url = requests.get(url,params=data)
        xml_parse = xmltodict.parse(url.text)
        self.api_token = xml_parse['resp']['out']['token']
        print('API Token Received!')

    def get_board_list(self):
        # localhost#FuncPosboard_getlist
        data = {
            'f': 'board_getlist',
            'tkn': self.api_token
        }

        url_item_list = f'localhost'
        url = requests.get(url_item_list,params=data)
        xml_parse = xmltodict.parse(url.text)
        xml_response = xml_parse['resp']['out']['boardlist']['boardinfo']
        self.df = pd.DataFrame(xml_response)
        self.df_for_items = self.df
        self.df.reset_index(drop=True, inplace=True)
        self.df = self.df[['id','code','revision','description','notes','customercode','bintable','active']]
        self.df.to_sql('BoardInfo', engine, if_exists='replace',schema='juki')
        print('BoardInfo Pushed!')

    def get_board_items(self):
        board_items = []
        alternative_board_items = []
        faulty_format = []
        for i,series in self.df_for_items.iterrows():
            try:
                df_board_item = pd.DataFrame(series['items']['boarditem'])
                if(len(df_board_item.alternativeitems.value_counts()) > 0) == False:
                    df_filter = df_board_item['filter'].apply(pd.Series)
                    df_consignment = df_board_item['consignment'].apply(pd.Series)
                    board_id = pd.Series(series['id'])
                    df_concat = pd.concat([board_id,df_board_item, df_filter,df_consignment], axis=1)
                    df_concat.rename(columns={0:'board_id'},inplace=True)
                    df_concat['board_id'] = pd.to_numeric(df_concat['board_id'], errors='coerce')
                    
                    # https://stackoverflow.com/questions/65263207/valueerror-repeats-may-not-contain-negative-values
                    df_concat = df_concat.ffill()
                    
                    # handling 2 supplier columns

                    cols = []
                    count = 1
                    for column in df_concat.columns:
                        if column == 'supplier':
                            cols.append(f'supplier_{count}')
                            count+=1
                            continue
                        cols.append(column)
                    df_concat.columns = cols

                    df_concat = df_concat.drop(['filter','consignment'], axis=1)
                    df_concat.rename(columns={'supplier_1':'supplier','supplier_2':'ConsignmentSupplier','priority':'ConsignmentPriority'},inplace=True)
                    board_items.append(df_concat)
                else:
                    df_board_item_alternative = pd.DataFrame(series['items']['boarditem']['alternativeitems']['alternativeitem'])
                    df_board_item_alternative_filter = df_board_item_alternative['filter'].apply(pd.Series)
                    df_board_item_alternative_consignment = pd.DataFrame(series['items']['boarditem']['consignment'],index=[0])
                    board_id_alternative = pd.Series(series['id'])
                    df_concat_alternative = pd.concat([board_id_alternative,df_board_item_alternative,df_board_item_alternative_filter,df_board_item_alternative_consignment], axis=1)
                    df_concat_alternative.rename(columns={0:'board_id'},inplace=True)
                    df_concat_alternative['board_id'] = pd.to_numeric(df_concat_alternative['board_id'], errors='coerce')

                    df_concat_alternative = df_concat_alternative.ffill()

                    cols = []
                    count = 1
                    for column in df_concat_alternative.columns:
                        if column == 'supplier':
                            cols.append(f'supplier_{count}')
                            count+=1
                            continue
                        cols.append(column)
                    df_concat_alternative.columns = cols

                    df_concat_alternative = df_concat_alternative.drop(['filter','priority','supplier_2'], axis=1)
                    df_concat_alternative.rename(columns={'supplier_1':'supplier','id':'itemid','code':'itemcode'},inplace=True)
                    df_concat_alternative['OrderPref'] = np.arange(df_concat_alternative.shape[0])
                    df_concat_alternative = df_concat_alternative[['board_id','itemid','itemcode','OrderPref','supplier','mpn','manufacturer']]
                    alternative_board_items.append(df_concat_alternative)
            except:
                faulty_format.append(series['id'])
        final_board = pd.concat(board_items)
        final_board.reset_index(drop=True, inplace=True)
        final_board.to_sql('BoardItem', engine, if_exists='replace',schema='juki')
        print('BoardItem Pushed!')

        final_board_alternative = pd.concat(alternative_board_items)
        final_board_alternative.reset_index(drop=True, inplace=True)
        final_board_alternative.to_sql('BoardAlternativeItems', engine, if_exists='replace',schema='juki')
        print('BoardAlternativeItmes Pushed!')

    def get_item_info(self):
        # localhost#FuncPositem_getlist
        data = {
            'f': 'item_getlist',
            'tkn': self.api_token
        }

        url_item_list = f'localhost'
        url = requests.get(url_item_list,params=data)
        xml_parse = xmltodict.parse(url.text)
        xml_response = xml_parse['resp']['out']['itemlist']['iteminfo']
        df = pd.DataFrame(xml_response)
        df.reset_index(drop=True, inplace=True)
        df.to_sql('ItemInfo', engine, if_exists='replace',schema='juki')
        print('ItemInfo Pushed!')

    def get_reel_info(self):
        # localhost#FuncPosreel_getlist
        data = {
            'f': 'reel_getlist',
            'tkn': self.api_token
        }

        url_item_list = f'localhost'
        url = requests.get(url_item_list,params=data)
        xml_parse = xmltodict.parse(url.text)
        xml_response = xml_parse['resp']['out']['reellist']['reelinfo']
        df = pd.DataFrame(xml_response)
        df[['adddate', 'adduser']] = df['add'].str.split(',', 1, expand=True)
        df.drop('add',inplace=True,axis=1)
        df.reset_index(drop=True, inplace=True)
        df.to_sql('ReelInfo', engine, if_exists='replace',schema='juki')
        print('ReelInfo Pushed!')

    def get_session_list(self):
        # localhost#FuncPossession_getlist

        data = {
            'f': 'session_getlist',
            'tkn': self.api_token
        }

        url_item_list = f'localhost'
        url = requests.get(url_item_list,params=data)
        xml_parse = xmltodict.parse(url.text)
        xml_response = xml_parse['resp']['out']['sessions']['sessioninfo']
        self.session_list_df = pd.DataFrame(xml_response)

    def get_session_boards(self):
        # localhost#FuncPossession_getboards

        session_boards = []
        no_session_boards = []

        for i in self.session_list_df['id']:
            data = {
                'f': 'session_getboards',
                'tkn': self.api_token,
                'id': i
            }

            url_item_list = f'localhost'
            url = requests.get(url_item_list,params=data)
            xml_parse = xmltodict.parse(url.text)
            try:     
                xml_response = xml_parse['resp']['out']['sessionboards']['sessionboard']
                df_session_board = pd.DataFrame(xml_response,index=[0])
                df_session_board['session_id'] = i
                df_session_board = df_session_board.ffill()
                first_column = df_session_board.pop('session_id')
                df_session_board.insert(0, 'session_id', first_column)
                session_boards.append(df_session_board)
            except:
                no_session_boards.append(i)
        final_session_boards = pd.concat(session_boards)
        final_session_boards.reset_index(drop=True, inplace=True)
        final_session_boards.to_sql('SessionBoard', engine, if_exists='replace',schema='juki')
        print('SessionBoard Pushed!')

    def get_session_info(self):
        # localhost#FuncPossession_getinfo
        
        session_info = []
        no_session_info = []

        for i in self.session_list_df['id']:
            data = {
                'f': 'session_getinfo',
                'tkn': self.api_token,
                'id': i
            }

            url_item_list = f'localhost'
            url = requests.get(url_item_list,params=data)
            xml_parse = xmltodict.parse(url.text)
            try:
                xml_response = xml_parse['resp']['out']['info']
                df_session_info = pd.DataFrame(xml_response,index=[0])
                df_session_info = df_session_info.ffill()
                session_info.append(df_session_info)
            except:
                no_session_info.append(i)
        final_session_info = pd.concat(session_info)
        final_session_info.reset_index(drop=True, inplace=True)
        final_session_info.to_sql('SessionInfo', engine, if_exists='replace',schema='juki')
        print('SessionInfo Pushed!')

    def get_session_items(self):
        # localhost#FuncPossession_getitems
        session_items = []
        no_session_items = []

        for i in self.session_list_df['id']:
            data = {
                'f': 'session_getitems',
                'tkn': self.api_token,
                'id': i
            }

            url_item_list = f'localhost'
            url = requests.get(url_item_list,params=data)
            xml_parse = xmltodict.parse(url.text)
            try:
                xml_response = xml_parse['resp']['out']['sessionitems']['sessionitem']
                df_initial = pd.DataFrame(xml_response)
                df_item_info = df_initial['iteminfo'].apply(pd.Series)
                df_session_item = pd.concat([df_initial, df_item_info], axis=1, join='inner')
                df_session_item = df_session_item.drop('iteminfo', axis=1)
                
                df_session_item['session_id'] = i
                df_session_item = df_session_item.ffill()
                first_column = df_session_item.pop('session_id')
                df_session_item.insert(0, 'session_id', first_column)
                session_items.append(df_session_item)
            except:
                no_session_items.append(i)
        final_session_items = pd.concat(session_items)
        final_session_items.rename(columns={'id':'ItemId'},inplace=True)
        final_session_items.reset_index(drop=True, inplace=True)
        final_session_items.to_sql('SessionItem', engine, if_exists='replace',schema='juki')
        print('SessionItem Pushed!')

    def get_session_reels(self):
        # localhost#FuncPossession_getreels
        
        session_reels = []
        no_sessions = []
        for i in self.session_list_df['id']:
            data = {
                'f': 'session_getreels',
                'tkn': self.api_token,
                'id': i
            }

            url_item_list = f'localhost'
            url = requests.get(url_item_list,params=data)
            xml_parse = xmltodict.parse(url.text)
            try:
                xml_response = xml_parse['resp']['out']['sessionreels']['sessionreel']
                df = pd.DataFrame(xml_response)
                df['session_id'] = i
                df = df.ffill()
                first_column = df.pop('session_id')
                df.insert(0, 'session_id', first_column)
                session_reels.append(df)
            except:
                no_sessions.append(i) 
        final_session_reels = pd.concat(session_reels)
        final_session_reels.reset_index(drop=True, inplace=True)
        final_session_reels.to_sql('SessionReel', engine, if_exists='replace',schema='juki')
        print('SessionReel Pushed!')
        conn.close()

s = StorageSolutionsData()
s.get_token()
s.get_board_list()
s.get_board_items()
s.get_item_info()
s.get_reel_info()
s.get_session_list()
s.get_session_boards()
s.get_session_info()
s.get_session_items()
s.get_session_reels()
