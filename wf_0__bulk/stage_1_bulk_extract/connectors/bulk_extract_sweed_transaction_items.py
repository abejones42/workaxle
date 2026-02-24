import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:.2f}'.format
from google.cloud import bigquery
client = bigquery.Client()

# Querying Transaction Items Items Table
###############################################################################################
query_job   = client.query( " SELECT InvoiceID, InvoiceItemID FROM `dev-verano-data-warehouse.s3_sweed.staging_dealer_211170__raw__sale__InvoiceItem_v` " )
raw_data    = query_job.to_dataframe()

raw_data.to_csv('workaxle/Data/main_raw/sweed/main_raw_sweed_transaction_items.csv', date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False)
