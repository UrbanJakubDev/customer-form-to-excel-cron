import os
import pandas as pd
from dotenv import load_dotenv

from utils import create_excel


class GasWatcher():

    def __init__(self) -> None:
        load_dotenv()
        self.save_path = os.getenv('FILE_STORAGE_PATH_GW')
        self.interval = '*/30'

    def __call__(self):
        self.run()

    def create_job(self, aps):
        aps.add_job(self.run, 
                    trigger='cron',
                    minute=self.interval,
                    id='gas_watcher', replace_existing=True)

    def get_data(self):
        from database import DBConnection
        db = DBConnection.getInstance()
        engine = db.getEngine()
        sql = """
      select
         date(from_unixtime(ldr.date_record)) as 'datetime',
         ke.eic,
         ldr.Plyn_All_VB as 'celkova spotreba m3',
         ldr.Plyn_VB as 'spotreba kgj m3'
      from
         ld_d_records ldr
      left join kgj_eic ke on
         ke.device_id = ldr.id_kgj
      where
         (ldr.id_kgj = 157 or ldr.id_kgj = 43)
         and year(from_unixtime(ldr.date_record)) = '2023'
      """
        df = pd.read_sql(sql, engine)
        return df

    def save_data(self, df):
        create_excel(df, 'plyn_ld.xlsx', self.save_path)

    def run(self):
        data = self.get_data()
        
        # Calculate difference between rows
        data['diff'] = data['celkova spotreba m3'].diff()
        data['diff kgj'] = data['spotreba kgj m3'].diff()
        
        # Find when column eic changes value (new measurement) and set diff to 0
        data['diff'] = data['diff'].where(data['eic'] == data['eic'].shift(), 0)
        data['diff kgj'] = data['diff kgj'].where(data['eic'] == data['eic'].shift(), 0)
        
        self.save_data(data)
