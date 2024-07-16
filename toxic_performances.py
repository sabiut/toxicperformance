import pandas as pd
import psycopg2
from decimal import Decimal
import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import os
import boto3
from botocore.exceptions import ClientError


class TraderDataHandler:
    """
    Downloads a pickle file from an S3 bucket.

    Args:
        self (object): The instance of the class that this method belongs to.

    Raises:
        ClientError: If there is an error downloading the file from S3, such as the file not existing in the bucket.
    """

    def __init__(self, pkl_file_path, s3_bucket_name, s3_file_key, db_config, slack_token, slack_channel):
        self.pkl_file_path = pkl_file_path
        self.s3_bucket_name = s3_bucket_name
        self.s3_file_key = s3_file_key
        self.db_config = db_config
        self.slack_token = slack_token
        self.slack_channel = slack_channel
        self.slack_client = WebClient(token=slack_token)

    def download_pickle_from_s3(self):
     
        s3_client = boto3.client('s3')
        try:
            s3_client.download_file(
                self.s3_bucket_name, self.s3_file_key, self.pkl_file_path)
            print(
                f"File {self.s3_file_key} downloaded from S3 bucket {self.s3_bucket_name}.")
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(
                    f"The file {self.s3_file_key} does not exist in the bucket {self.s3_bucket_name}.")
            else:
                raise e

    def merge_data(self, df1, df2, merge_column, merge_type='left'):
        return pd.merge(df1, df2, on=merge_column, how=merge_type)

    def load_traders(self):
        try:
            return pd.read_pickle(self.pkl_file_path)
        except FileNotFoundError:
            return pd.DataFrame()

    def prepare_and_fetch_flagged_data(self):
        df_screened = self.load_traders()
        login_screened = [str(login)
                          for login in df_screened["login"].tolist()]
        values_string = ', '.join([f"('{login}', '{reason}', '{flag_time}')" for login, reason, flag_time in zip(
            login_screened, df_screened['reason'], df_screened['flag_time'])])

        query_flag = f"""
        CREATE TEMP TABLE temp_login_flag_time (
            login VARCHAR(255),
            reason VARCHAR(255),
            flag_time TIMESTAMP
        );
        INSERT INTO temp_login_flag_time (login, reason,flag_time)
        VALUES {values_string};
        with mtx_trades as (
                SELECT login ,sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live1".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live2".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all            
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live3".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live4".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all            
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live5".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all            
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live6".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all
                select login, sum(volume*0.0001) as vol, sum(profit+"storage"+commission) as net_pnl, left("time",10) as date
                from "titan-mt5live1".mt5_deals
                where entry in (0,1)
                and action in (0,1)
                group by login , left("time",10)
        ), mtx_users as (
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live1".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live2".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live3".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live4".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live5".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live6".mt4_users
                union all
                select u.login, acc.equity ,u.email, country, registration AS reg_date, u.agent, LEFT(u."group",3) AS acc_ccy, RIGHT(u."group",1) AS book from "titan-mt5live1".mt5_users u inner join mt5live1_mt5_accounts acc on acc.login = u.login
        ), mtx as (
            SELECT t.login, u.email, u.country, reg_date, u.equity, t.vol, t.net_pnl as net_pnl, u.acc_ccy, u.book, "date", u.agent, d.reason,d.flag_time
            from mtx_trades t
            inner join mtx_users u on u.login = t.login
            INNER JOIN temp_login_flag_time d ON t.login = d.login
            WHERE t.login IN ({', '.join(f"'{login}'" for login in login_screened)})
            AND t."date" >= d.flag_time
        ), ccy_convert as (
            select t.*,
            ceil(100 * t.net_pnl * (json_extract_path_text(r.data, 'USD')::FLOAT / json_extract_path_text(r.data, t.acc_ccy)::FLOAT)) /100.0 as net_pnl_usd
            from mtx t
            inner join tfxdata_daily_usd_rates r
            on t."date" = r."date"
        ), mtx_last as (
        SELECT login, email, equity, book, country, reg_date, agent, acc_ccy, SUM(vol) as vol_flag, SUM(net_pnl_usd) as net_pnl_flag_usd, reason, flag_time
        FROM ccy_convert
        GROUP BY login, email, equity, country, reg_date,book, agent, acc_ccy, reason, flag_time
        )
        select * from mtx_last
        """
        df_final = self.fetch_data_from_db(query_flag)
        df_final.rename(columns={
            'login': 'Login',
            'email': 'EMail',
            'equity': 'Equity',
            'book': 'Book',
            'country': 'Country',
            'reg_date': 'Registration Date',
            'agent': 'IB',
            'acc_ccy': 'Account Currency',
            'vol_flag': 'Volume since Flagged',
            'net_pnl_flag_usd': 'Net PNL since Flagged (USD)',
            'reason': 'Reason',
            'flag_time': 'Flag Time',
            'vol_reg': 'All Volume',
            'net_pnl_reg_usd': 'All Net PNL (USD)'
        }, inplace=True)
        return df_final

    def prepare_and_fetch_registration_data(self):
        df_screened = self.load_traders()
        login_screened = [str(login)
                          for login in df_screened["login"].tolist()]

        query_reg = f"""
                with mtx_trades as (
                SELECT login ,sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live1".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live2".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all            
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live3".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live4".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all            
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live5".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all            
                SELECT login, sum(volume*0.01) as vol, sum(profit+swaps+commission) as net_pnl, left(close_time,10) as date
                FROM  "titan-mt4live6".mt4_trades
                where cmd in (0, 1)
                group by login , left(close_time,10)
                union all
                select login, sum(volume*0.0001) as vol, sum(profit+"storage"+commission) as net_pnl, left("time",10) as date
                from "titan-mt5live1".mt5_deals
                where entry in (0,1)
                and action in (0,1)
                group by login , left("time",10)
        ), mtx_users as (
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live1".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live2".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live3".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live4".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live5".mt4_users
                union all
                select login, equity, email, country, regdate AS reg_date, agent_account AS agent, LEFT("group",3) AS acc_ccy, RIGHT("group",1) AS book from "titan-mt4live6".mt4_users
                union all
                select u.login, acc.equity ,u.email, country, registration AS reg_date, u.agent, LEFT(u."group",3) AS acc_ccy, RIGHT(u."group",1) AS book from "titan-mt5live1".mt5_users u inner join mt5live1_mt5_accounts acc on acc.login = u.login
        ), mtx as (
            SELECT t.login, t.vol, t.net_pnl as net_pnl, u.acc_ccy, "date"
            from mtx_trades t
            inner join mtx_users u on u.login = t.login
            WHERE t.login IN ({', '.join(f"'{login}'" for login in login_screened)})
        ), ccy_convert as (
            select t.*,
            ceil(100 * t.net_pnl * (json_extract_path_text(r.data, 'USD')::FLOAT / json_extract_path_text(r.data, t.acc_ccy)::FLOAT)) /100.0 as net_pnl_usd
            from mtx t
            inner join tfxdata_daily_usd_rates r
            on t."date" = r."date"
        ), mtx_last as (
        SELECT login, SUM(vol) as vol_reg, SUM(net_pnl_usd) as net_pnl_reg_usd
        FROM ccy_convert
        GROUP BY login
        )
        select * from mtx_last
        """
        return self.fetch_data_from_db(query_reg)

    def fetch_data_from_db(self, query):
        conn = psycopg2.connect(**self.db_config)
        cur = conn.cursor()
        cur.execute(query)
        df = pd.DataFrame(cur.fetchall(), columns=[
                          desc[0] for desc in cur.description])
        cur.close()
        conn.close()
        return df

    def write_df_to_csv(self, df, csv_file_path):
        df.to_csv(csv_file_path, index=False)
        print(f"Data successfully written to CSV at {csv_file_path}")

    def send_csv_to_slack(self, csv_file_path):
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        initial_comment = f"Hi, please find below the Toxic Performance report as of {current_date}"
        
        try:
            with open(csv_file_path, "rb") as file_content:
                response = self.slack_client.files_upload_v2(
                    channels=self.slack_channel,
                    file=file_content,
                    filename=os.path.basename(csv_file_path),
                    title="Toxic Performance Data",
                    initial_comment=initial_comment
                )
            print("File uploaded to Slack channel successfully.")
        except SlackApiError as e:
            print(f"Error uploading file to Slack: {e.response['error']}")



