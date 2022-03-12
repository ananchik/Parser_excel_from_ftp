# -*- coding: utf-8 -*-

"""
Created on Mon Mar  7 10:14:09 2022

@author: denis.safronov
"""
from urllib.request import urlretrieve
from io import BytesIO
import urllib 
import pandas as pd
import sys
import numpy as np
import time
import os
from openpyxl import load_workbook
from psycopg2 import Error
import re
import openpyxl 
import cx_Oracle
from datetime import datetime
import dateutil.parser
import ftplib
from ftplib import FTP_TLS
import ftputil
import pysftp 
import os
from io import BytesIO
import logging
import xlrd


######### CONNECT TO ORACLE ###########
# try:
#     cx_Oracle.init_oracle_client(lib_dir='C:\\Users\\denis.safronov\\OneDrive - Accenture\\Documents\\sqldeveloper\\instantclient_19_9')
# except:
#     print('client has already existed')
    
    
def connections():
    con = cx_Oracle.connect(user='', password='', dsn='', encoding="UTF-8")
    return con

def call_sequence():
    try:
        connection = connections()
        cursor = connection.cursor()
        
        get_seq_sql = """select xxlm_ship_piton_head_seq.nextval from dual"""
        cursor.execute(get_seq_sql)
        record = cursor.fetchall()
        return record[0][0]
        
    except (Exception, Error) as error:
        print("Error while connecting to ORACLE", error)
        
        with open("error_log_insert_to_db.txt",'a+', encoding = 'utf-8') as f:
            f.write('xxlm_ship_piton_head_seq.nextval from dual' + str(error)+ '\n')
    finally:
        if (connection):
            cursor.close()
            connection.close()
   
            
def getId_data_from_PITON_HEAD():
    try:
        connection = connections()
        cursor = connection.cursor()
        
        get_seq_sql = """select ID from XXLM_SHIP_PITON_HEAD
                         order by ID desc"""
        
        cursor.execute(get_seq_sql) # dateutil.parser.parse()
        record = cursor.fetchone()
        return record[0]
    
#         connection.commit()
    except (Exception, Error) as error:
        print("Error while connecting to ORACLE", error)
        
        with open("error_log_insert_to_db.txt",'a+', encoding = 'utf-8') as f:
            f.write('XXLM_SHIP_PITON_HEAD ' + str(error) + '\n')
    finally:
        if (connection):
            cursor.close()
            connection.close()
            
def insert_data_to_PITON_HEAD(data, file, comments):

    try:
        connection = connections()
        cursor = connection.cursor()
        status = "NEW"
        commen_new = comments
        
        if str(data.at[0, 'дата отгрузки']) == 'NaT' or str(data.at[0, 'дата отгрузки']) == '' or str(data.at[0, 'дата отгрузки']) =='nan': # or str(data.at[0, 'дата отгрузки']) =='nan' ADDED 11/03/2022 10:21
            data.at[0, 'дата отгрузки'] = dateutil.parser.parse(time.strftime("%Y.%m.%d %H:%m"))
        
        if str(data.at[0, 'Дата ТТН']) == 'NaT' or str(data.at[0, 'Дата ТТН']) == '' or str(data.at[0, 'Дата ТТН']) == 'None' or str(data.at[0, 'Дата ТТН']) == 'nan': # or str(data.at[0, 'Дата ТТН']) == 'nan' ADDED 11/03/2022 10:21
            
            if str(data.at[0, 'ТТН']) == 'None' or str(data.at[0, 'ТТН']) == 'nan' or str(data.at[0, 'ТТН']) == '' :
                get_seq_sql = """INSERT INTO "XXLM_SHIP_PITON_HEAD"("ID","SHIP_DATE", "STATUS", "ORDER_NO", "HANDLING_MODE", 
                                                                "CREATE_DATE", "TEMPERATURE_MODE", "TTN", "TTN_DATE", "WAYBILL", "COMMENTS")
                             VALUES (:id, :shipdate, :status, :order_no, :handle_mode, :create_date, :temper_mode, :ttn, :ttn_date, :waybill, :comm)""" # , :comm 

                cursor.execute(get_seq_sql, [call_sequence(),
                                         data.at[0, 'дата отгрузки'], 
                                         status, 
                                         int(data.at[0, 'номер заказа']), 
                                         data.at[0, 'Боковая разгрузка машины (ДА/НЕТ)'], 
                                         dateutil.parser.parse(time.strftime("%Y.%m.%d %H:%m")),
                                         str(data.at[0, 'Температура хранения (5,17, НЕТ)']), 
                                         '', #str(data.at[0, 'ТТН']), 
                                         '', #data.at[0, 'Дата ТТН'], 
                                         str(data.at[0,'Номер накладной ТОРГ-12']),
                                         commen_new ]) 
                connection.commit()
            else:    
                get_seq_sql = """INSERT INTO "XXLM_SHIP_PITON_HEAD"("ID","SHIP_DATE", "STATUS", "ORDER_NO", "HANDLING_MODE", 
                                                                "CREATE_DATE", "TEMPERATURE_MODE", "TTN", "TTN_DATE", "WAYBILL", "COMMENTS")
                             VALUES (:id, :shipdate, :status, :order_no, :handle_mode, :create_date, :temper_mode, :ttn, :ttn_date, :waybill, :comm)""" # , :comm 

                cursor.execute(get_seq_sql, [call_sequence(),
                                         data.at[0, 'дата отгрузки'], 
                                         status, int(data.at[0, 'номер заказа']), 
                                         data.at[0, 'Боковая разгрузка машины (ДА/НЕТ)'], 
                                         dateutil.parser.parse(time.strftime("%Y.%m.%d %H:%m")),
                                         str(data.at[0, 'Температура хранения (5,17, НЕТ)']), 
                                         str(data.at[0, 'ТТН']), 
                                         '',  #data.at[0, 'Дата ТТН'], 
                                         str(data.at[0,'Номер накладной ТОРГ-12']),
                                         commen_new ]) 
                connection.commit()
            

        else:
            get_seq_sql = """INSERT INTO "XXLM_SHIP_PITON_HEAD"("ID","SHIP_DATE", "STATUS", "ORDER_NO", "HANDLING_MODE", 
                                                                "CREATE_DATE", "TEMPERATURE_MODE", "TTN", "TTN_DATE", "WAYBILL", "COMMENTS")
                             VALUES (:id, :shipdate, :status, :order_no, :handle_mode, :create_date, :temper_mode, :ttn, :ttn_date, :waybill, :comm )""" # , :comm

            cursor.execute(get_seq_sql, [call_sequence(),
                                         data.at[0, 'дата отгрузки'], 
                                         status, int(data.at[0, 'номер заказа']), 
                                         data.at[0, 'Боковая разгрузка машины (ДА/НЕТ)'], 
                                         dateutil.parser.parse(time.strftime("%Y.%m.%d %H:%m")),
                                         str(data.at[0, 'Температура хранения (5,17, НЕТ)']), 
                                         str(data.at[0, 'ТТН']), 
                                         dateutil.parser.parse(data.at[0, 'Дата ТТН']), 
                                         str(data.at[0,'Номер накладной ТОРГ-12']),
                                         commen_new  ]) 
                                        # dateutil.parser.parse() 
                                    
            connection.commit()
    except (Exception, Error) as error:
        print("Error while connecting to ORACLE", error)
        #add_files_to_dir_error(file)   # ВОТ ТУТ ПОДЛЯНКА ПРОВЕРИТЬ НЕ ЗАБУДЬ !!!!! - РАБОТАЕТ!!! ПЕРЕКИДЫВАЕТ В ERROR
        # return error
        
        with open("error_log_insert_to_db.txt",'a+', encoding = 'utf-8') as f:
            f.write('XXLM_SHIP_PITON_HEAD ' + str(file) + str(error) + '\n')

        add_files_to_dir_error(file) # переместил сюда из строчки где был return
    finally:
        if (connection):
            cursor.close()
            connection.close()
            
def insert_data_to_PITON_DETAIL(data, file):
        
    try:
        connection = connections()
        cursor = connection.cursor()
        for i in range(len(data)):
            get_seq_sql = """INSERT INTO XXLM_SHIP_PITON_DETAIL ( "HEADER_ID", "SSCC", "ITEM", "QTY", 
                                                              "PACKAGING_CODE", "WEIGHT", "LENGTH", "WIDTH")
                         VALUES (:header_id, :SSCC, :item, :qty, :pkg_code, :weight, :lenght, :width)"""
        
        
            cursor.execute(get_seq_sql, [getId_data_from_PITON_HEAD(),
                                         str(data.at[i, 'Серийный номер (SSCC)']), int(data.at[i, 'Код товара у покупателя']),
                                         int(data.at[i, 'Количество отгружено']), str(data.at[i, 'Тип упаковки']), 
                                         float(data.at[i, 'Вес паллеты, кг']), float(data.at[i, 'Длина паллеты, см']), 
                                         float(data.at[i, 'Ширина паллеты, см']) ])
            connection.commit()
        
    except (Exception, Error) as error:
        print("Error while connecting to ORACLE", error)
        
        with open("error_log_insert_to_db.txt",'a+', encoding = 'utf-8') as f:
            f.write('XXLM_SHIP_PITON_DETAIL ' + str(file) + str(error)+ '\n')
        add_files_to_dir_error(file)
    finally:
        if (connection):
            cursor.close()
            connection.close()

######## CONNECT TO FTP ################

            
def get_files_list_ftp():

    #host = ''
    #port_1 = 990
    USER  = ''
    PASS = ''
    #enc = 'utf-8'

    try:
        with ftplib.FTP_TLS() as ftp:   #ftp=FTP_TLS()
        #ftp.set_debuglevel(2)
            ftp.connect('', 990)
            ftp.login(user=USER, passwd=PASS)
            ftp.prot_p() 
            ftp.cwd('/IN')
            excel_files_ftp = ftp.nlst('*.xls')
            
        return excel_files_ftp
            
    except:
        print('Не удалось получить список файлов из ftp-сервера func get_files_list_ftp')
        

def add_files_to_dir_processed(file_name):
    #file_name = 'C:\\error_log.txt'
    #host = ''
    #port_1 = 990
    USER  = ''
    PASS = ''
    #enc = 'utf-8'
    
    try:
        with ftplib.FTP_TLS() as ftp:   #ftp=FTP_TLS()
    
            ftp.connect('', 990)
            ftp.login(user=USER, passwd=PASS)
            ftp.prot_p()
            filepathSource = '/IN/'+file_name
            filepathDestination = '/IN/Processed/'+file_name
            ftp.rename(filepathSource, filepathDestination)

    except:
        print('ERROR func add_files_to_dir_processed and file - ', file_name, '. Go to File error_log_insert_to_db.txt')
      
def add_files_to_dir_error(file_name):
    
    #file_name = 'C:\\pattern-for-supplier.xlsx'
    #host = ''
    #port_1 = 990
    USER  = ''
    PASS = ''
    
    try:
        with ftplib.FTP_TLS() as ftp:   #ftp=FTP_TLS()
    
            ftp.connect('', 990)
            ftp.login(user=USER, passwd=PASS)
            ftp.prot_p()
            filepathSource = '/IN/'+file_name
            filepathDestination = '/IN/Error/'+file_name
            ftp.rename(filepathSource, filepathDestination)

    except:
        print('ERROR func add_files_to_dir_error and file - ', file_name, '. Go to File error_log_insert_to_db.txt')
            
        
###### РАЗБИВКА СПИСКА ##############        

def func_chunk(lst, n):
    
    for x in range(0, len(lst), n):
        e_c = lst[x : n + x]

        if len(e_c) < n:
            e_c = e_c + [None for y in range(n - len(e_c))]
        yield e_c
        
############ START MAIN CODE ##################################################


if __name__ == "__main__":
    
    USER  = ''
    PASS = ''
        #enc = 'utf-8'
    
    with ftplib.FTP_TLS() as ftp:   #ftp=FTP_TLS()
    #ftp.set_debuglevel(2)
        ftp.connect('', 990)
        ftp.login(user=USER, passwd=PASS)
        ftp.prot_p() 
        ftp.cwd('/IN')
        excel_files_ftp = ftp.nlst('*.xls')
        #print(excel_files_ftp)
        
        for unprocessed_file in range(len(excel_files_ftp)):
            try:
                clean_filename = os.path.splitext(excel_files_ftp[unprocessed_file])[0]
                extension = str(os.path.splitext(excel_files_ftp[unprocessed_file])[1]).lower()
                
                file_object = BytesIO()
                ftp.retrbinary('RETR '+excel_files_ftp[unprocessed_file], file_object.write)
        

                
                if extension in ('.xls'): #, '.xlsx'
                    workbook =  xlrd.open_workbook(file_contents=file_object.getvalue())

                    sheet = workbook.sheet_by_index(0)
                    sheetnames = workbook.sheet_names()
                
                    name_supp = sheet.cell_value(rowx = 0, colx = 2) + ' ' + sheet.cell_value(rowx = 0, colx = 4)
                    # print(name_supp)
                    # print(type(name_supp))
                
                    arr_colms = []
                    arr_values = []
                
                    for i in range(1, sheet.row_len(2)): # перебор столбцов для создания заголовка таблицы
                        try:
                            arr_colms.append(sheet.cell_value(rowx = 2, colx = i))
                        except: continue
                
                    for j in range(3, sheet.nrows):
                        for i in range(1, sheet.row_len(2)): # перебор столбцов
                            if i==2:
                                arr_values.append(str(sheet.cell_value(rowx = j, colx = i)))
                            elif i == 4: # ПОСТАВИТЬ ОГРАНИЧЕНИЯ НА ДАТУ ЧТОБЫ СТАВИЛАСЬ СИСТЕМНАЯ!!!!

                                cell = sheet.cell_value(rowx = j, colx = i)
                                    #print(cell)
                                    #print(type(cell))
                                
                                if cell == '':
                                        #print('вошел в ковычки')
                                    arr_values.append('None')
                                else:
                                        #print('вошел в else')
                                    arr_values.append(datetime(*xlrd.xldate_as_tuple(cell, workbook.datemode)))
                                        #print(datetime(*xlrd.xldate_as_tuple(cell, workbook.datemode)))
                                    #arr_values.append(datetime(*xlrd.xldate_as_tuple(sheet.cell_value(rowx = j, colx = i), workbook.datemode)))
                                    # datetime.datetime(*xlrd.xldate_as_tuple(a1, book.datemode))

                                # arr_values.append(datetime(*xlrd.xldate_as_tuple(sheet.cell_value(rowx = j, colx = i), workbook.datemode)))
                                # # datetime.datetime(*xlrd.xldate_as_tuple(a1, book.datemode))
                            elif i == 9:
                                if sheet.cell_value(rowx = j, colx = i) == str(sheet.cell_value(rowx = j, colx = i)):
                                    arr_values.append(str(sheet.cell_value(rowx = j, colx = i)))
                                else:
                                    arr_values.append(str(int(sheet.cell_value(rowx = j, colx = i))))
                            else:    
                                arr_values.append(sheet.cell_value(rowx = j, colx = i))
                
                    arr_values1 = list(func_chunk(arr_values, len(arr_colms)))
                    df = pd.DataFrame(arr_values1, columns=arr_colms)
                    
                
                    df.loc[(df['Боковая разгрузка машины (ДА/НЕТ)'] == 'НЕТ'), 'Боковая разгрузка машины (ДА/НЕТ)'] = 'NO'
                    df.loc[(df['Боковая разгрузка машины (ДА/НЕТ)'] == 'ДА'), 'Боковая разгрузка машины (ДА/НЕТ)'] = 'YES'
                
                    df.loc[(df['Температура хранения (5,17, НЕТ)'] == 'НЕТ'), 'Температура хранения (5,17, НЕТ)'] = 'NO'
                    df.loc[(df['Температура хранения (5,17, НЕТ)'] == 'ДА'), 'Температура хранения (5,17, НЕТ)'] = 'YES'
                
                    df['ТТН'] = df['ТТН'].astype(str)
                    
                    # Добавление логики на удаление пустых строк в 
                    
                    # 11/03/2022 10:20 ADDED
                    df = df.replace('', 'None')
                    df = df.replace(to_replace='None', value=np.nan).dropna(how='all')
                    
                    ##############################################################
                
                    new_df_group_by_ship = df[['Номер накладной ТОРГ-12', 'номер заказа']].value_counts().reset_index()

                    for i in range(len(new_df_group_by_ship)):
                        columns_tab_head = ['Номер накладной ТОРГ-12', 'ТТН', 'Дата ТТН', 'дата отгрузки','номер заказа',
                                            'Боковая разгрузка машины (ДА/НЕТ)', 'Температура хранения (5,17, НЕТ)']
                        columns_tab_detail = ['Серийный номер (SSCC)', 'Тип упаковки', 'Вес паллеты, кг', 'Длина паллеты, см',
                                                'Ширина паллеты, см', 'Код товара у покупателя', 'Количество отгружено']
                        m = new_df_group_by_ship[0][i] # кол-во каждой сгруппированной строки по упд и номеру заказа
                
                        new_df_for_each_ship = df[((df['Номер накладной ТОРГ-12'] == new_df_group_by_ship['Номер накладной ТОРГ-12'][i]) & (df['номер заказа'] == new_df_group_by_ship['номер заказа'][i]))] 
                        new_df_for_each_ship.reset_index(inplace=True)

                        values_tab_head = []
                        for row in range(len(new_df_for_each_ship)):
                            if row > 0: continue
                            else:
                                values_tab_head.append([new_df_for_each_ship.at[row,'Номер накладной ТОРГ-12'], 
                                                        new_df_for_each_ship.at[row,'ТТН'],
                                                        new_df_for_each_ship.at[row,'Дата ТТН'],  
                                                        new_df_for_each_ship.at[row,'дата отгрузки'], 
                                                        new_df_for_each_ship.at[row,'номер заказа'],
                                                        new_df_for_each_ship.at[row,'Боковая разгрузка машины (ДА/НЕТ)'], 
                                                        new_df_for_each_ship.at[row,'Температура хранения (5,17, НЕТ)'] ])
                
                            df_final = pd.DataFrame(values_tab_head,columns=columns_tab_head)
                            
                            # ЗАПИСЬ В БД ТАБЛИЦА HEAD
                            try:
                                insert_data_to_PITON_HEAD(df_final, excel_files_ftp[unprocessed_file], name_supp ) 
                            except:
                                add_files_to_dir_error(excel_files_ftp[unprocessed_file])
                
                            count = 0 
                            values_tab_detail = []
                            while count < m:
                                values_tab_detail.append([new_df_for_each_ship.at[count, 'Серийный номер (SSCC)'], 
                                                            new_df_for_each_ship.at[count, 'Тип упаковки'],
                                                            new_df_for_each_ship.at[count, 'Вес паллеты, кг'], 
                                                            new_df_for_each_ship.at[count, 'Длина паллеты, см'],
                                                            new_df_for_each_ship.at[count, 'Ширина паллеты, см'],
                                                            new_df_for_each_ship.at[count, 'Код товара у покупателя'],
                                                            new_df_for_each_ship.at[count, 'Количество отгружено'] ])
                
                                count += 1
                
                            df_final_detail = pd.DataFrame(values_tab_detail, columns=columns_tab_detail)
                
                            # ЗАПИСЬ В БД ТАБЛИЦА DETAIL
                            try:
                                insert_data_to_PITON_DETAIL(df_final_detail, excel_files_ftp[unprocessed_file] ) 
                            except:
                                add_files_to_dir_error(excel_files_ftp[unprocessed_file])
                                
                    # Перемещения файла на ftp сервере в папку processed
                    add_files_to_dir_processed(excel_files_ftp[unprocessed_file]) 
                        
            except:
                print('Наименовая файла, который не обработался - ', excel_files_ftp[unprocessed_file])
                
                # Перемещения файла на ftp сервере в папку error
                add_files_to_dir_error(excel_files_ftp[unprocessed_file])
       















