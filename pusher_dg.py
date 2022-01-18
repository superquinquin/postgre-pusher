import os
import re
import sys
import csv
import datetime
import openpyxl
import traceback
import unicodedata

import psycopg2

from keys import key


class pusher:
  def __init__(self, path):
    try:
      # self.path = '/'.join(os.path.dirname(__file__).split('/')[:-1])
      self.path = path
      self.log = open(os.path.dirname(__file__)+'/log.txt','a', encoding='utf-8', errors='ignore')
      self.table_list = []

      self.write_log('init pusher', False,'', '')

    except Exception as e:
      self.write_log('init error', True, e, '')



  def connect(self, database, user, password, host, port):
    try:
      self.conn = psycopg2.connect(host=host,
                                   port=port,
                                   database=database,
                                   user=user,
                                   password=password)

      self.cursor = self.conn.cursor()
      
      self.write_log('connection done', False,'', '')

    except Exception as e:
      self.write_log('connection error', True, e, '')



  def close(self):
    try:
      self.log.close()
      self.conn.close()

    except Exception:
      pass

  def write_log(self, msg, trace_back, e, add):
    self.log.write('--'+datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")+'-- '+msg+' -- '+add+'\n')

    if trace_back == True:
      self.log.write('--'+datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")+'-- '+str(e)+'\n'+str(traceback.extract_tb(sys.exc_info()[2]))+'\n')




  def table_set(self):
    try:
      table_query = "SELECT * FROM information_schema.tables;"
      self.cursor.execute(table_query)
      self.table_list = list(self.cursor.fetchall())

    except Exception as e:
      self.write_log('fetch error', True, e, '')


  def name_cleaner(self, name):
    name = ''.join([x for x in  unicodedata.normalize('NFD', name) if unicodedata.category(x) != 'Mn']).lower()
    name = re.sub(r'[^.a-zA-Z0-9]', "_", name)
    name = re.sub(r'_{2,}','_', name)
    
    return name


  def read_csv(self, path):
    doc = open(path, encoding='utf-8', errors='ignore')
    csvfile = csv.reader(doc, delimiter=',')

    columns, content = [], []
    for n,row in enumerate(csvfile):
      if n == 0:
        columns.extend(row)
      else:
        content.append(row)

    return columns, content


  def read_excel(self, path):
    doc = openpyxl.load_workbook(path).active

    columns, content = [], []
    rows, cols = doc.max_row, doc.max_column

    for row in range(1,rows+1):
      r = []
      for col in range(1, cols+1):
        r.append(doc.cell(row = row, column = col).value)
      
      if row == 1:
        columns.extend(r)
      else:
        content.append(r)
    return columns, content



  def correct_bool(self, content, columns):
    match_bool = re.compile(r'vrai|faux|true|false')

    for idx in range(len(columns)):
      col = [x[idx] for x in content]
      uniq = list(set(col))
      
      matched = list(filter(None,[re.findall(match_bool, x.lower())  for x in uniq if type(x) == str]))

      if (len(uniq) == 2 and len(matched) == 2) or (len(uniq) == 1 and len(matched) == 1):
        for row in content:

          if row[idx].lower() in ['vrai','true']:
            row[idx] = True

          else:
            row[idx] = False
            
    return content


  def csv_preprocessing(self, path, file):
    name = self.name_cleaner(file.split('.')[0])
    columns, content = self.read_csv(path+'/'+file)
    content = self.correct_bool(content, columns)

    return name, columns, content



  def xlsx_preprocessing(self, path, file):
    name = self.name_cleaner(file.split('.')[0])
    columns, content = self.read_excel(path+'/'+file)
    content = self.correct_bool(content, columns)
    
    return name, columns, content



  def label_data_type(self, columns, content):
    match1 = re.compile(r'\d+-\d+-\d+')
    match2 = re.compile(r'(\d+/\d+/\d+)')
    match3 = re.compile(r'(\d+\.\d+\.\d+)')
    match_bool = re.compile(r'vrai|faux|true|false')

    sql_cols_type, sql_val_format, sql_cols = [], [], []
    for idx in range(len(columns)):

      col_name = self.name_cleaner(columns[idx])
      sql_cols.append(col_name)

      col = [x[idx] for x in content]

      atype = None
      for i in col:
        if i is not  None or (type(i)== str and i.lower() != 'none'):
          atype = type(i)
          break


      if atype == datetime.datetime:
        sql_val_format.append('%s')
        sql_cols_type.append(col_name + ' DATE')

      elif atype == str:
        for i in col:
          if i is not  None or (type(i)== str and i.lower() != 'none'):
            exmpl = i
            break

        if re.findall(match1,exmpl) or re.findall(match2,exmpl) or re.findall(match3,exmpl):
          sql_cols_type.append(col_name + ' DATE')
          sql_val_format.append('%s')

        elif re.findall(match_bool, exmpl.lower()):
          sql_cols_type.append(col_name + ' BOOL')
          sql_val_format.append('%s')
        
        else:
          sql_cols_type.append(col_name + ' TEXT')
          sql_val_format.append('%s')        


      elif atype == int:
        sql_cols_type.append(col_name + ' INT8')
        sql_val_format.append('%s')

      elif atype == float:
        sql_cols_type.append(col_name + ' FLOAT8')
        sql_val_format.append('%s')

      elif atype == bool:
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



  def query_create_table(self,name, sql_cols_type):
    create_query = "CREATE TABLE IF NOT EXISTS "+name+" ("+sql_cols_type+")"
    self.cursor.execute(create_query)
    self.conn.commit()

  def query_delete_content(self, name):
    delete_query = "DELETE FROM "+name
    self.cursor.execute(delete_query)
    self.conn.commit()


  def query_drop_table(self, name):
    drop_query = "DROP TABLE "+name
    self.cursor.execute(drop_query)
    self.conn.commit()


  def query_push_content(self, name, sql_cols, sql_val_format, content):
    push_query = "INSERT INTO "+name+" ("+sql_cols+") VALUES ("+sql_val_format+")"
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
              self.query_drop_table(name)
              self.query_create_table(name, sql_cols_type)
              self.query_push_content(name, sql_cols, sql_val_format, content)

            else:
              self.query_create_table(name, sql_cols_type)
              self.query_push_content(name, sql_cols, sql_val_format, content)

            self.write_log(file, False, '', 'pushed sucessfully')

          except Exception as e:
            self.write_log('payload error', True, e, '')


        elif extension == 'xlsx':
          try:
            name, columns, content = self.xlsx_preprocessing(path, file)
            sql_cols_type, sql_val_format, sql_cols = self.label_data_type(columns, content)
            in_place = self.search_table(name)

            if in_place == True:
              self.query_drop_table(name)
              self.query_create_table(name, sql_cols_type)
              self.query_push_content(name, sql_cols, sql_val_format, content)

            else:
              self.query_create_table(name, sql_cols_type)
              self.query_push_content(name, sql_cols, sql_val_format, content)

            self.write_log(file, False, '', 'pushed sucessfully')

          except Exception as e:
            self.write_log('payload error', True, e, '') 
  

  
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
  odoo_pusher = pusher(key['path']).run(key['database'], 
                                        key['username'],
                                        key['password'],
                                        key['host'],
                                        key['port'])