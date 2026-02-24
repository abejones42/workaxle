import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:.2f}'.format
from google.cloud import bigquery
client = bigquery.Client()

# Creating Transaction Items Table
###############################################################################################
query_job   = client.query( """
                            SELECT
                                SI.InvoiceID,
                                SI.DealerID,
                                SI.InvoiceIssuingTypeID,
                                SI.InvoiceStatusID,
                                US.UserID,
                                CD.DealerName,
                                SI.LocalPayTime 
                            FROM `dev-verano-data-warehouse.s3_sweed.staging_dealer_211170__raw__sale__Invoice_v` AS SI
                                INNER JOIN (
                                        SELECT
                                        UserID,
                                        UserSessionID
                                    FROM `dev-verano-data-warehouse.s3_sweed.staging_dealer_211170__raw__shifts__UserSession_v`
                                ) AS US ON SI.UserSessionID = US.UserSessionID
                                INNER JOIN (
                                        SELECT 
                                        DealerID,
                                        DealerName
                                    FROM `dev-verano-data-warehouse.s3_sweed.staging_dealer_211170__raw__companies__Dealer_v`
                                ) AS CD ON SI.DealerID = CD.DealerID
                            WHERE SI.UserSessionID > 0
                            """ )
raw_data    = query_job.to_dataframe()

raw_data.to_csv('workaxle/Data/main_raw/sweed/main_raw_sweed_transactions.csv', date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False)
