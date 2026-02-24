import warnings
warnings.simplefilter('ignore')
import boto3
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:.2f}'.format

s3 = boto3.client( 's3', aws_access_key_id="", aws_secret_access_key="")

# Datetime Object
import datetime
from pytz import timezone
cst = timezone('US/Central')
mst = timezone('US/Mountain')
est = timezone('US/Eastern')
utc = timezone('UTC')

master_location_table   = pd.read_csv("workaxle/Data/controls/verano_locations.csv", low_memory=False)

# READING RAW Transactions
###############################################################################################
def read_large_file(file_path, chunk_size, column_dtypes):
    for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype=column_dtypes):
        yield chunk

file_path_list = [
    'workaxle/Data/main_raw/connecticut/main_raw_dutchie_transactions.csv',
    'workaxle/Data/main_raw/massachusetts/main_raw_dutchie_transactions.csv',
    'workaxle/Data/main_raw/michigan/main_raw_dutchie_transactions.csv',
    'workaxle/Data/main_raw/ohio/main_raw_dutchie_transactions.csv',
    'workaxle/Data/main_raw/virginia/main_raw_dutchie_transactions.csv',
]

staging_transactions_table = pd.DataFrame({})
for file_path in file_path_list:
    chunk_size = 100000000
    column_dtypes   = {"dutchie_transaction_id":str}
    for transactions_chunk in read_large_file(file_path, chunk_size, column_dtypes):

        # Changing Data Types
        ###############################################################################################
        transactions_chunk["time"] = transactions_chunk["dutchie_transaction_date_local_time"].astype("datetime64[ns]").dt.strftime("%Y-%m-%dT%H:00:00")
        transactions_chunk["dutchie_transaction_order_source"] = transactions_chunk["dutchie_transaction_order_source"].str.replace("IHeartJane","Online").replace("Walk In","In Store").replace("Internal","Online").replace("Leafly","Online").replace("Weedmaps","Online")

        staging_transactions_table = pd.concat([staging_transactions_table, transactions_chunk], ignore_index=True)

dutchie_master_store_name   = staging_transactions_table[["master_code", "dutchie_transaction_id", "dutchie_transaction_employee_id", "dutchie_transaction_order_source", "time"]].drop_duplicates().reset_index(drop=True)
staging_transactions_table  = dutchie_master_store_name.merge(master_location_table, left_on="master_code", right_on="department_id", how="left")[["branch_id", "department_id", "dutchie_transaction_id", "dutchie_transaction_employee_id", "dutchie_transaction_order_source", "time", "timezone"]]
staging_transactions_table["time"]  = staging_transactions_table["time"] + staging_transactions_table["timezone"]
staging_transactions_table  = staging_transactions_table[[ "branch_id", "department_id", "dutchie_transaction_id", "dutchie_transaction_employee_id", "dutchie_transaction_order_source", "time"]].rename(columns={ "dutchie_transaction_order_source":"demand_type"})
# print(staging_transactions_table.count())

staging_dutchie_transactions_agg_table = staging_transactions_table.groupby([
        "branch_id",
        "department_id",
        "demand_type",
        "time",
    ]).agg(
        demand                  = pd.NamedAgg(column="dutchie_transaction_id", aggfunc="count"),
        amount_of_budtenders    = pd.NamedAgg(column="dutchie_transaction_employee_id", aggfunc="nunique"),
    ).reset_index()
staging_dutchie_transactions_agg_table
# print(staging_dutchie_transactions_agg_table.shape)

date = datetime.datetime.now(tz=cst).strftime("%Y-%m-%d")
staging_dutchie_transactions_agg_table.to_csv(f"workaxle/Data/main_reports/agg_verano_transactions_{date}.csv", date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False)

# READING RAW Sweed Transactions
###############################################################################################
file_path   = "workaxle/Data/main_raw/sweed/main_raw_sweed_transactions.csv"
chunk_size  = 100000000
column_dtypes   = {"InvoiceID":str}
for staging_dealer_211170__raw__sale__Invoice_v in read_large_file(file_path, chunk_size, column_dtypes):
    staging_dealer_211170__raw__sale__Invoice_v["time"] = staging_dealer_211170__raw__sale__Invoice_v["LocalPayTime"].astype("datetime64[ns]").dt.strftime("%Y-%m-%dT%H:00:00")
    staging_dealer_211170__raw__sale__Invoice_v['date'] = staging_dealer_211170__raw__sale__Invoice_v['LocalPayTime'].astype('datetime64[ns]').dt.strftime("%Y-%m-%d")
    staging_dealer_211170__raw__sale__Invoice_v['hour'] = staging_dealer_211170__raw__sale__Invoice_v['LocalPayTime'].astype('datetime64[ns]').dt.strftime("%H")
    staging_dealer_211170__raw__sale__Invoice_v         = staging_dealer_211170__raw__sale__Invoice_v.rename(columns={ "InvoiceIssuingTypeID":"demand_type", "InvoiceStatusID":"demand_status" })
    staging_dealer_211170__raw__sale__Invoice_v["demand_type"]      = staging_dealer_211170__raw__sale__Invoice_v["demand_type"].astype(str).replace("1", "In Store").replace("2", "Online").replace("3", "In Store").replace("4", "In Store").replace("5", "In Store").replace("6", "Online").replace("7", "Online").replace("8", "Online").replace("9", "In Store").replace("10", "In Store")
    break

# READING RAW Sweed Locations Table Including Location Code
###############################################################################################
stage_raw_transactions_data = staging_dealer_211170__raw__sale__Invoice_v[staging_dealer_211170__raw__sale__Invoice_v["demand_status"] == 3][["DealerID", "DealerName"]].drop_duplicates()
# print(stage_raw_transactions_data.head(2))

dealerState = { "branch_id": list(), "department_id": list(), "DealerID": list(), "timezone": list() }
for index, row in stage_raw_transactions_data.iterrows():
    DealerID    = row["DealerID"]
    DealerName  = row["DealerName"]

    stage_name = DealerName.split(" - ")[0].strip()
    for index, row in master_location_table[master_location_table['name'].str.contains(pat=stage_name)].iterrows():
        dealerState["branch_id"].append(row["branch_id"])
        dealerState["department_id"].append(row["department_id"])
        dealerState["timezone"].append(row["timezone"])
        dealerState["DealerID"].append(DealerID)
sweed_location_w_code_table = pd.DataFrame(dealerState)

merge_dealer_211170__raw__sale__Invoice_v = staging_dealer_211170__raw__sale__Invoice_v.merge(sweed_location_w_code_table, on="DealerID", how="left")
merge_dealer_211170__raw__sale__Invoice_v["time"]  = merge_dealer_211170__raw__sale__Invoice_v["time"] + merge_dealer_211170__raw__sale__Invoice_v["timezone"]

# Creating Sweed Transaction Aggregate Table
###############################################################################################
staging_sweed_transactions_agg_table = merge_dealer_211170__raw__sale__Invoice_v.groupby([ "branch_id", "department_id", "demand_type", "time" ]).agg( demand = pd.NamedAgg(column="InvoiceID", aggfunc="count"), amount_of_budtenders = pd.NamedAgg(column="UserID", aggfunc="nunique")).reset_index()
# print(staging_sweed_transactions_agg_table.shape)
date = datetime.datetime.now(tz=cst).strftime("%Y-%m-%d")
staging_sweed_transactions_agg_table.to_csv(f"workaxle/Data/main_reports/agg_verano_transactions_{date}.csv", date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False, header=False, mode="a")

s3.upload_file(f'workaxle/Data/main_reports/agg_verano_transactions_{date}.csv', 'verano-jarvis', f'workaxle/reports/agg_verano_transactions_{date}.csv')
