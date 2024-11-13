import sqlite3
import pandas as pd
import random
import os

class ExcelManager:

    def get_data_from_file(name='Приложение_к_заданию_бек_разработчика.xlsx', sheet_name='Лист1'):
        if os.path.exists(name):
            data = pd.read_excel(name, sheet_name='Лист1', index_col=None, header=2)
            headers = data.columns.tolist()
            #print(headers)
            result_data_list = []
            for row in list(data.itertuples(index=False, name=None)):
                row = [item for item in row]
                #Добавляем дату в формате 'день.месяц.год'
                row.append(f'{random.randint(1, 5)}.11.2024')
                result_data_list.append(row)
            return result_data_list
        else:
            return []
            
class DatabaseManager:
    
    def __init__(self, name="bd.sqlite3", table = "statistics"):
        self.bd_name = name
        self.table_name = table
        self.con = sqlite3.connect(self.bd_name)
       
       
    def calculate_totals(self):
        cursor = self.con.cursor()
        cursor.execute(f"""
            SELECT date, 
                SUM(fact_qlid_data1 + fact_qlid_data2) AS total_fact_qlid,
                SUM(fact_qoil_data1 + fact_qoil_data2) AS total_fact_qoil,
                SUM(forecast_qlid_data1 + forecast_qlid_data2) AS total_forecast_qlid,
                SUM(forecast_qoil_data1 + forecast_qoil_data2) AS total_forecast_qoil
            FROM {self.table_name}
            GROUP BY date
        """)
        totals = cursor.fetchall()
        for row in totals:
            print(f"""
                Date: {row[0]},
                Total Fact Qlid: {row[1]},
                Total Fact Qoil: {row[2]},
                Total Forecast Qlid: {row[3]},
                Total Forecast Qoil: {row[4]}.""")
            

    def create_table(self,):
        cursor = self.con.cursor()
        #date в формате 'день.месяц.год'
        cursor.execute(f"""
            CREATE TABLE {self.table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,  
                company TEXT, 
                fact_qlid_data1 INTEGER,
                fact_qlid_data2 INTEGER,
                fact_qoil_data1 INTEGER,
                fact_qoil_data2 INTEGER,
                forecast_qlid_data1 INTEGER,
                forecast_qlid_data2 INTEGER,
                forecast_qoil_data1 INTEGER,
                forecast_qoil_data2 INTEGER,
                date TEXT
            )""")
        self.con.commit()
        
        
    def upload_data(self, data):
        if data:
            cursor = self.con.cursor()
            cursor.executemany(f"""
                INSERT INTO {self.table_name}
                    (id, company, 
                    fact_qlid_data1, fact_qlid_data2,
                    fact_qoil_data1, fact_qoil_data2,
                    forecast_qlid_data1, forecast_qlid_data2,
                    forecast_qoil_data1, forecast_qoil_data2,
                    date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", data)
            self.con.commit()


def init_bd(db_manager):
    try:
        data=ExcelManager.get_data_from_file()
        db_manager.create_table() 
        db_manager.upload_data(data=data)
    except:
        pass
        
if __name__ == '__main__':
    db_manager = DatabaseManager()
    init_bd(db_manager)
    db_manager.calculate_totals()
