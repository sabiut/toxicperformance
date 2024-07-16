import os
from dotenv import load_dotenv
from toxic_performances import TraderDataHandler
import pandas as pd

load_dotenv()


def handler():
    db_config = {
        'host': os.getenv('DATABASE_HOST'),
        'dbname': os.getenv('DATABASE_NAME'),
        'user': os.getenv('DATABASE_USER'),
        'password': os.getenv('DATABASE_PASSWORD'),
        'port': os.getenv('DATABASE_PORT')
    }
    
    slack_token = os.getenv('TOXIC_SLACK_API_TOKEN')
    slack_channel = os.getenv('TOXIC_SLACK_CHANNEL')
    s3_bucket_name = os.getenv('S3_BUCKET_NAME')
    s3_file_key = os.getenv('S3_FILE_KEY')
    pkl_file_path = os.getenv('PKL_FILE_PATH')

    handler = TraderDataHandler(pkl_file_path, s3_bucket_name, s3_file_key, db_config, slack_token, slack_channel)

    # Ensure the pickle file is downloaded from S3
    handler.download_pickle_from_s3()

    # Now proceed with loading and processing the data
    df_flag = handler.prepare_and_fetch_flagged_data()
    df_reg = handler.prepare_and_fetch_registration_data()

    # Since columns are renamed, use the new column names
    df_final = pd.merge(df_flag, df_reg, left_on='Login', right_on='login', how='left')
    
    if df_final.empty:
        print("DataFrame is empty!")
    else:
        csv_file_path = './ToxicPerformanceData.csv'
        handler.write_df_to_csv(df_final, csv_file_path)
        handler.send_csv_to_slack(csv_file_path)


if __name__ == "__main__":
    handler()
