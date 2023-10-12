import os
import pandas as pd
from dotenv import load_dotenv

from utils import create_excel


class GasWatcher():

    def __init__(self) -> None:
        load_dotenv()
        self.save_path = os.getenv('FILE_STORAGE_PATH_GW')
        self.interval = '*/30'
        self.devices = [
            {
                'id': 157,
                'gas': 1
            },
            {
                'id': 43,
                'gas': 2
            },
            {
                'id': 129,
                'gas': 2
            }]

    def __call__(self):
        self.run()

    def create_job(self, aps):
        aps.add_job(self.run,trigger='cron',minute=self.interval,id='gas_watcher', replace_existing=True)
        # aps.add_job(self.run, trigger="interval", minutes=1, id='gas_watcher', replace_existing=True)
        print('Gas watcher job created')
        
    def make_query(self, device, year):
        # The column type depends on whether the device uses gas or electricity
        column_type = 'Plyn_VB' if device['gas'] == 1 else 'Plyn_All_VB'
        month_queries = ''
        
        # Create a query for each month
        for month in range(1, 13):
            month_query = f"(Max(CASE WHEN month(from_unixtime(ldr.date_record)) = '{month}' then ldr.{column_type}  END) - Min(CASE WHEN month(from_unixtime(ldr.date_record)) = '{month}' then ldr.{column_type}  END)) AS '{month}',"
            month_queries += month_query
            
        # Remove last comma
        month_queries = month_queries[:-1]
        
        return f"""
            select
                ke.eic,
                date(from_unixtime(ldr.date_record)) as 'datetime',
                {month_queries}
            from
                ld_d_records ldr
            left join kgj_eic ke on
                ke.device_id = ldr.id_kgj
            where
                year(from_unixtime(ldr.date_record)) = '{year}' and ldr.id_kgj = {device['id']}
            group by ldr.id_kgj
 """

    def get_data(self):
        from database import DBConnection
        db = DBConnection.getInstance()
        engine = db.getEngine()
        
        df = pd.DataFrame()
        for device in self.devices:
            sql = self.make_query(device, 2023)
            data = pd.read_sql(sql, engine)
            print(data)
            # Append data to df
            df = pd.concat([df, data])
    
        # Sum rows with same eic except datetime
        try:
            df = df.groupby(['eic', 'datetime']).sum().reset_index()
        except Exception as e:
            print(e)
            
        return df

    def save_data(self, df):
        create_excel(df, 'plyn_ld.xlsx', self.save_path)

    def run(self):
        
        try:
            data = self.get_data()
        except Exception as e:
            print(e)

        try:
            self.save_data(data)
        except Exception as e:
            print(e)
            print('Trying to save data again')

        print(f'Gas watcher job done. Data saved to {self.save_path} at {pd.Timestamp.now()}')
