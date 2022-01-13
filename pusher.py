import os
import sys
import traceback
import datetime
import re
import unicodedata

import psycopg2
import pandas as pd

from keys import key


class pusher:
  def __init__(self):
    self.path = '/'.join(os.path.dirname(__file__).split('/')[:-1])
    self.log = open(self.path+'/log.txt','a', encoding='utf-8', errors='ignore')
    self.table_list = []

    self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- init pusher\n')



  def connect(self, database, user, password, host, port):
    try:
      self.conn = psycopg2.connect(host=host,
                                   port=port,
                                   database=database,
                                   user=user,
                                   password=password)

      self.cursor = self.conn.cursor()
      
      self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- conection done\n')
    except Exception as e:
      self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- conection error\n')
      self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- {e}\n {traceback.extract_tb(sys.exc_info()[2])}\n')



  def close(self):
    self.log.close()
    self.conn.close()



  def table_set(self):
    try:
      table_query = "SELECT * FROM information_schema.tables;"
      self.cursor.execute(table_query)
      self.table_list = list(self.cursor.fetchall())

    except Exception as e:
      self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- fetch error\n')
      self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- {e}\n {traceback.extract_tb(sys.exc_info()[2])}\n')




  def name_cleaner(self, name):
    name = ''.join([x for x in  unicodedata.normalize('NFD', name) if unicodedata.category(x) != 'Mn']).lower()
    name = re.sub(r'[^.a-zA-Z0-9]', "_", name)
    name = re.sub(r'_{2,}','_', name)
    
    return name



  def correct_bool(self, df):
    match_bool = re.compile(r'vrai|faux|true|false')
    
    for col in list(df.columns):
      content = df[col].values.tolist()
      uniq = list(set(content))
      matched = list(filter(None,[re.findall(match_bool, x.lower())  for x in uniq if type(x) == str]))
      
      if (len(uniq) == 2 and len(matched) == 2) or (len(uniq) == 1 and len(matched) == 1):
        corrected = [True if x.lower() in ['vrai','true'] else False for x in content]
        df[col] = corrected    

    return df



  def csv_preprocessing(self, path, file):
    name = self.name_cleaner(file.split('.')[0])
    df = pd.read_csv(path+'/'+file)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = self.correct_bool(df)

    content = [tuple(x) for x in df.to_numpy()]

    return name, df, content



  def xlsx_preprocessing(self, path, file):
    name = self.name_cleaner(file.split('.')[0])
    df = pd.read_excel(path+'/'+file)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = self.correct_bool(df)

    content = [tuple(x) for x in df.to_numpy()]

    return name, df, content



  def label_data_type(self, df):
    """small support for date combination"""

    cols = list(df.columns)
    dtypes = list(df.dtypes)

    # dates format and bool format
    match1 = re.compile(r'\d+-\d+-\d+')
    match2 = re.compile(r'(\d+/\d+/\d+)')
    match3 = re.compile(r'(\d+\.\d+\.\d+)')
    match_bool = re.compile(r'vrai|faux|true|false')

    # label data type
    sql_cols_type, sql_val_format, sql_cols = [], [], []
    for n,elm in enumerate(dtypes):
      col_name = self.name_cleaner(cols[n])
      sql_cols.append(col_name)

      if elm.kind == 'M':
        sql_val_format.append('%s')
        sql_cols_type.append(col_name + ' DATE')

      elif elm.kind == 'O':
        exmpl = df.loc[df[str(cols[n])].notnull(), [str(cols[n])]].values.tolist()[0][0]

        if re.findall(match1,exmpl) or re.findall(match2,exmpl) or re.findall(match2,exmpl):
          sql_cols_type.append(col_name + ' DATE')
          sql_val_format.append('%s')

        elif re.findall(match_bool, exmpl.lower()):
          sql_cols_type.append(col_name + ' BOOL')
          sql_val_format.append('%s')

        elif elm.kind == 'O':
          sql_cols_type.append(col_name + ' TEXT')
          sql_val_format.append('%s')

      elif elm.kind == 'i' or elm.kind == 'u':
        sql_cols_type.append(col_name + ' INT8')
        sql_val_format.append('%s')

      elif elm.kind == 'f':
        sql_cols_type.append(col_name + ' FLOAT8')
        sql_val_format.append('%s')

      elif elm.kind == '?':
        sql_cols_type.append(col_name + ' BOOL')
        sql_val_format.append('%s')

      else:
        sql_cols_type.append(col_name + ' TEXT')
        sql_val_format.append('%s')

    sql_val_format = ', '.join(sql_val_format)
    sql_cols_type = ', '.join(sql_cols_type)
    sql_cols = ', '.join(sql_cols)

    return sql_cols_type, sql_val_format, sql_cols



  def search_table(self, name):
    in_place = False
    if name in [x[2] for x in self.table_list]:
      in_place = True

    return in_place



  def traceback_on_payload(self, e, file):
    self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- payload error -- {file}\n')
    self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- {e}\n {traceback.extract_tb(sys.exc_info()[2])}\n')       



  def query_create_table(self,name, sql_cols_type):
    create_query = f"CREATE TABLE IF NOT EXISTS {name} ({sql_cols_type})"
    self.cursor.execute(create_query)
    self.conn.commit()

  def query_delete_content(self, name):
    delete_query = f"DELETE FROM {name}"
    self.cursor.execute(delete_query)
    self.conn.commit()

  def query_push_content(self, name, sql_cols, sql_val_format, content):
    push_query = f"INSERT INTO {name} ({sql_cols})  VALUES ({sql_val_format})"
    self.cursor.executemany(push_query, content)
    self.conn.commit()



  def launch_payload(self, path):
    architecture = os.listdir(path)
    
    for file in architecture:

      if len(file.split('.')) == 1: 
        sub_path = self.path+'/'+file
        self.launch_payload(sub_path)

      else:
        extension = file.split('.')[1]

        if extension == 'csv':
          try:
            name, df, content = self.csv_preprocessing(path, file)
            sql_cols_type, sql_val_format, sql_cols = self.label_data_type(df)
            in_place = self.search_table(name)

            if in_place == True:
              self.query_delete_content(name)
              self.query_push_content(name, sql_cols, sql_val_format, content)

            else:
              self.query_create_table(name, sql_cols_type)
              self.query_push_content(name, sql_cols, sql_val_format, content)

            self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- {file} -- pushed sucessfully\n')

          except Exception as e:
            self.traceback_on_payload(e, file)

        elif extension == 'xlsx':
          try:
            name, df, content = self.xlsx_preprocessing(path, file)
            sql_cols_type, sql_val_format, sql_cols = self.label_data_type(df)
            in_place = self.search_table(name)

            if in_place == True:
              self.query_delete_content(name)
              self.query_push_content(name, sql_cols, sql_val_format, content)

            else:
              self.query_create_table(name, sql_cols_type)
              self.query_push_content(name, sql_cols, sql_val_format, content)

            self.log.write(f'--{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}-- {file} -- pushed sucessfully\n')

          except Exception as e:
            self.traceback_on_payload(e, file)  
  

  
  def run(self, database, username, password, host, port):
    self.connect(database, 
                 username,
                 password,
                 host,
                 port)
    self.table_set()
    self.launch_payload(self.path)
    self.close()





if __name__ == '__main__':
  odoo_pusher = pusher().run(key['database'], 
                             key['username'],
                             key['password'],
                             key['host'],
                             key['port'])

