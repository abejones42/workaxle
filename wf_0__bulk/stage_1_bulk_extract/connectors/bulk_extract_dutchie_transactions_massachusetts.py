import asyncio
import warnings
warnings.simplefilter('ignore')
import pandas as pd
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.options.display.float_format = '{:.2f}'.format
import datetime
from pytz import timezone
cst = timezone('US/Central')
mst = timezone('US/Mountain')
est = timezone('US/Eastern')
utc = timezone('UTC')

from Server.connectors.dutchie_api_get_transactions import dutchie_api_get_transactions

location_api_key    = pd.read_csv("workaxle/Data/controls/location_name_w_code.csv", low_memory=False)
filtered_location_api_key = location_api_key[location_api_key['state'] == "Massachusetts"]

dailyTransactionState = {
    "dutchie_transaction_id": list(),
    "master_state": list(),
    "master_code": list(),
    "master_store_name": list(),
    "dutchie_transaction_adjustment_for_transaction_id": list(),
    "dutchie_transaction_auth_code": list(),
    "dutchie_transaction_cash_paid": list(),
    "dutchie_transaction_change_due": list(),
    "dutchie_transaction_check_in_date": list(),
    "dutchie_transaction_check_paid": list(),
    "dutchie_transaction_completed_by_user": list(),
    "dutchie_transaction_credit_paid": list(),
    "master_customer_id": list(),
    "dutchie_transaction_customer_type_id": list(),
    "dutchie_transaction_debit_paid": list(),
    "dutchie_transaction_electronic_paid": list(),
    "dutchie_transaction_electronic_payment_method": list(),
    "dutchie_transaction_employee_id": list(),
    "dutchie_transaction_est_delivery_date_local": list(),
    "dutchie_transaction_est_time_arrival_local": list(),
    "dutchie_transaction_fees_and_donations": list(),
    "dutchie_transaction_gift_paid": list(),
    "dutchie_transaction_invoice_name": list(),
    "dutchie_transaction_invoice_number": list(),
    "dutchie_transaction_is_medical": list(),
    "dutchie_transaction_is_return": list(),
    "dutchie_transaction_is_tax_inclusive": list(),
    "dutchie_transaction_is_void": list(),
    "dutchie_transaction_last_modified_date_utc": list(),
    "dutchie_transaction_loyalty_earned": list(),
    "dutchie_transaction_loyalty_spent": list(),
    "dutchie_transaction_mmap_paid": list(),
    "dutchie_transaction_non_revenue_fees_and_donations": list(),
    "dutchie_transaction_order_ids": list(),
    "dutchie_transaction_order_method": list(),
    "dutchie_transaction_order_source": list(),
    "dutchie_transaction_order_type": list(),
    "dutchie_transaction_paid": list(),
    "dutchie_transaction_pre_payment_amount": list(),
    "dutchie_transaction_return_on_transaction_id": list(),
    "dutchie_transaction_revenue_fees_and_donations": list(),
    "dutchie_transaction_subtotal": list(),
    "dutchie_transaction_tax": list(),
    "dutchie_transaction_tax_summary": list(),
    "dutchie_transaction_terminal_name": list(),
    "dutchie_transaction_tip_amount": list(),
    "dutchie_transaction_total": list(),
    "dutchie_transaction_total_before_tax": list(),
    "dutchie_transaction_total_credit": list(),
    "dutchie_transaction_total_discount": list(),
    "dutchie_transaction_total_items": list(),
    "dutchie_transaction_date": list(),
    "dutchie_transaction_date_local_time": list(),
    "dutchie_transaction_type": list(),
    "dutchie_transaction_void_date": list(),
    "dutchie_transaction_was_pre_ordered": list(),
    "dutchie_transaction_integrationType": list(),
    "dutchie_transaction_integratedPaid": list(),
    "dutchie_transaction_externalPaymentId": list(),
}
dailyTransactionItemState = {
    "master_state": list(),
    "master_code": list(),
    "master_store_name": list(),
    "master_customer_id": list(),
    "dutchie_transaction_id": list(),
    "dutchie_transaction_item_id": list(),
    "dutchie_transaction_item_batch_name": list(),
    "dutchie_transaction_item_flower_equivalent": list(),
    "dutchie_transaction_item_flower_equivalent_unit": list(),
    "dutchie_transaction_item_is_coupon": list(),
    "dutchie_transaction_item_is_returned": list(),
    "dutchie_transaction_item_package_id": list(),
    "dutchie_transaction_item_product_id": list(),
    "dutchie_transaction_item_quantity": list(),
    "dutchie_transaction_item_return_date": list(),
    "dutchie_transaction_item_return_reason": list(),
    "dutchie_transaction_item_returned_by_transaction_id": list(),
    "dutchie_transaction_item_source_package_id": list(),
    "dutchie_transaction_item_taxes": list(),
    "dutchie_transaction_item_total_discount": list(),
    "dutchie_transaction_item_total_price": list(),
    "dutchie_transaction_item_unit_cost": list(),
    "dutchie_transaction_item_unit_id": list(),
    "dutchie_transaction_item_unit_price": list(),
    "dutchie_transaction_item_unit_weight": list(),
    "dutchie_transaction_item_unit_weight_unit": list(),
    "dutchie_transaction_item_vendor": list(),
}
dailyTransactionsDiscountState = {
    "master_state": list(),
    "master_code": list(),
    "master_store_name": list(),
    "master_customer_id": list(),
    "dutchie_transaction_discount_id": list(),
    "dutchie_transaction_discount_transaction_id": list(),
    "dutchie_transaction_discount_amount": list(),
    "dutchie_transaction_discount_name": list(),
    "dutchie_transaction_discount_reason": list(),
    "dutchie_transaction_discount_transaction_item_id": list(),
}
raw_daily_transactions_table            = pd.DataFrame(dailyTransactionState).to_csv("workaxle/Data/main_raw/massachusetts/main_raw_dutchie_transactions.csv", date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False)
raw_daily_transaction_item_table        = pd.DataFrame(dailyTransactionItemState).to_csv("workaxle/Data/main_raw/massachusetts/main_raw_dutchie_transaction_items.csv", date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False)
raw_daily_transactions_discount_table   = pd.DataFrame(dailyTransactionsDiscountState).to_csv("workaxle/Data/main_raw/massachusetts/main_raw_dutchie_transaction_discounts.csv", date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False)

hasMore         = True
start_date      = "2024-03-23"

while hasMore is True:
    start_date_dt   = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date        = datetime.datetime.strftime(start_date_dt + datetime.timedelta(days=1), '%Y-%m-%d')
    today_dt        = datetime.datetime.now(tz=est).date()

    dailyTransactionState = {
        "dutchie_transaction_id": list(),
        "master_state": list(),
        "master_code": list(),
        "master_store_name": list(),
        "dutchie_transaction_adjustment_for_transaction_id": list(),
        "dutchie_transaction_auth_code": list(),
        "dutchie_transaction_cash_paid": list(),
        "dutchie_transaction_change_due": list(),
        "dutchie_transaction_check_in_date": list(),
        "dutchie_transaction_check_paid": list(),
        "dutchie_transaction_completed_by_user": list(),
        "dutchie_transaction_credit_paid": list(),
        "master_customer_id": list(),
        "dutchie_transaction_customer_type_id": list(),
        "dutchie_transaction_debit_paid": list(),
        "dutchie_transaction_electronic_paid": list(),
        "dutchie_transaction_electronic_payment_method": list(),
        "dutchie_transaction_employee_id": list(),
        "dutchie_transaction_est_delivery_date_local": list(),
        "dutchie_transaction_est_time_arrival_local": list(),
        "dutchie_transaction_fees_and_donations": list(),
        "dutchie_transaction_gift_paid": list(),
        "dutchie_transaction_invoice_name": list(),
        "dutchie_transaction_invoice_number": list(),
        "dutchie_transaction_is_medical": list(),
        "dutchie_transaction_is_return": list(),
        "dutchie_transaction_is_tax_inclusive": list(),
        "dutchie_transaction_is_void": list(),
        "dutchie_transaction_last_modified_date_utc": list(),
        "dutchie_transaction_loyalty_earned": list(),
        "dutchie_transaction_loyalty_spent": list(),
        "dutchie_transaction_mmap_paid": list(),
        "dutchie_transaction_non_revenue_fees_and_donations": list(),
        "dutchie_transaction_order_ids": list(),
        "dutchie_transaction_order_method": list(),
        "dutchie_transaction_order_source": list(),
        "dutchie_transaction_order_type": list(),
        "dutchie_transaction_paid": list(),
        "dutchie_transaction_pre_payment_amount": list(),
        "dutchie_transaction_return_on_transaction_id": list(),
        "dutchie_transaction_revenue_fees_and_donations": list(),
        "dutchie_transaction_subtotal": list(),
        "dutchie_transaction_tax": list(),
        "dutchie_transaction_tax_summary": list(),
        "dutchie_transaction_terminal_name": list(),
        "dutchie_transaction_tip_amount": list(),
        "dutchie_transaction_total": list(),
        "dutchie_transaction_total_before_tax": list(),
        "dutchie_transaction_total_credit": list(),
        "dutchie_transaction_total_discount": list(),
        "dutchie_transaction_total_items": list(),
        "dutchie_transaction_date": list(),
        "dutchie_transaction_date_local_time": list(),
        "dutchie_transaction_type": list(),
        "dutchie_transaction_void_date": list(),
        "dutchie_transaction_was_pre_ordered": list(),
        "dutchie_transaction_integrationType": list(),
        "dutchie_transaction_integratedPaid": list(),
        "dutchie_transaction_externalPaymentId": list(),
    }
    dailyTransactionItemState = {
        "master_state": list(),
        "master_code": list(),
        "master_store_name": list(),
        "master_customer_id": list(),
        "dutchie_transaction_id": list(),
        "dutchie_transaction_item_id": list(),
        "dutchie_transaction_item_batch_name": list(),
        "dutchie_transaction_item_flower_equivalent": list(),
        "dutchie_transaction_item_flower_equivalent_unit": list(),
        "dutchie_transaction_item_is_coupon": list(),
        "dutchie_transaction_item_is_returned": list(),
        "dutchie_transaction_item_package_id": list(),
        "dutchie_transaction_item_product_id": list(),
        "dutchie_transaction_item_quantity": list(),
        "dutchie_transaction_item_return_date": list(),
        "dutchie_transaction_item_return_reason": list(),
        "dutchie_transaction_item_returned_by_transaction_id": list(),
        "dutchie_transaction_item_source_package_id": list(),
        "dutchie_transaction_item_taxes": list(),
        "dutchie_transaction_item_total_discount": list(),
        "dutchie_transaction_item_total_price": list(),
        "dutchie_transaction_item_unit_cost": list(),
        "dutchie_transaction_item_unit_id": list(),
        "dutchie_transaction_item_unit_price": list(),
        "dutchie_transaction_item_unit_weight": list(),
        "dutchie_transaction_item_unit_weight_unit": list(),
        "dutchie_transaction_item_vendor": list(),
    }
    dailyTransactionsDiscountState = {
        "master_state": list(),
        "master_code": list(),
        "master_store_name": list(),
        "master_customer_id": list(),
        "dutchie_transaction_discount_id": list(),
        "dutchie_transaction_discount_transaction_id": list(),
        "dutchie_transaction_discount_amount": list(),
        "dutchie_transaction_discount_name": list(),
        "dutchie_transaction_discount_reason": list(),
        "dutchie_transaction_discount_transaction_item_id": list(),
    }

    for location, api in filtered_location_api_key.iterrows():
        code            = api["code"]
        api_key         = api["api_key"]
        master_state    = api["state"]
        location_name   = api["location_name"]
        load_get_transactions = asyncio.run( dutchie_api_get_transactions(api_key, start_date, end_date, include_item_details="true") )
        for transaction in load_get_transactions:
            ts = transaction.get("transactionDate", None)
            try:
                dailyTransactionState["dutchie_transaction_date"].append(datetime.datetime.strptime(ts,"%Y-%m-%dT%H:%M:%S.%f").date())
            except:
                dailyTransactionState["dutchie_transaction_date"].append(datetime.datetime.strptime(ts,"%Y-%m-%dT%H:%M:%S").date())
            items = transaction["items"]
            discounts = transaction["discounts"]
            dailyTransactionState["master_code"].append(code)
            dailyTransactionState["master_state"].append(master_state)
            dailyTransactionState["master_store_name"].append(location_name)
            dailyTransactionState["dutchie_transaction_id"].append(str(transaction.get("transactionId", None)))
            dailyTransactionState["master_customer_id"].append(transaction.get("customerId", None))
            dailyTransactionState["dutchie_transaction_employee_id"].append(transaction.get("employeeId", None))
            dailyTransactionState["dutchie_transaction_void_date"].append(transaction.get("voidDate", None))
            dailyTransactionState["dutchie_transaction_is_void"].append(transaction.get("isVoid", None))
            dailyTransactionState["dutchie_transaction_subtotal"].append(transaction.get("subtotal", None))
            dailyTransactionState["dutchie_transaction_total_discount"].append(transaction.get("totalDiscount", None))
            dailyTransactionState["dutchie_transaction_total_before_tax"].append(transaction.get("totalBeforeTax", None))
            dailyTransactionState["dutchie_transaction_tax"].append(transaction.get("tax", None))
            dailyTransactionState["dutchie_transaction_tip_amount"].append(transaction.get("tipAmount", None))
            dailyTransactionState["dutchie_transaction_total"].append(transaction.get("total", None))
            dailyTransactionState["dutchie_transaction_paid"].append(transaction.get("paid", None))
            dailyTransactionState["dutchie_transaction_change_due"].append(transaction.get("changeDue", None))
            dailyTransactionState["dutchie_transaction_total_items"].append(transaction.get("totalItems", None))
            dailyTransactionState["dutchie_transaction_terminal_name"].append(transaction.get("terminalName", None))
            dailyTransactionState["dutchie_transaction_check_in_date"].append(transaction.get("checkInDate", None))
            dailyTransactionState["dutchie_transaction_invoice_number"].append(transaction.get("invoiceNumber", None))
            dailyTransactionState["dutchie_transaction_is_tax_inclusive"].append(transaction.get("isTaxInclusive", None))
            dailyTransactionState["dutchie_transaction_type"].append(transaction.get("transactionType", None))
            dailyTransactionState["dutchie_transaction_loyalty_earned"].append(transaction.get("loyaltyEarned", None))
            dailyTransactionState["dutchie_transaction_loyalty_spent"].append(transaction.get("loyaltySpent", None))
            dailyTransactionState["dutchie_transaction_last_modified_date_utc"].append(transaction.get("lastModifiedDateUTC", None))
            dailyTransactionState["dutchie_transaction_cash_paid"].append(transaction.get("cashPaid", None))
            dailyTransactionState["dutchie_transaction_debit_paid"].append(transaction.get("debitPaid", None))
            dailyTransactionState["dutchie_transaction_electronic_paid"].append(transaction.get("electronicPaid", None))
            dailyTransactionState["dutchie_transaction_electronic_payment_method"].append(transaction.get("electronicPaymentMethod", None))
            dailyTransactionState["dutchie_transaction_check_paid"].append(transaction.get("checkPaid", None))
            dailyTransactionState["dutchie_transaction_credit_paid"].append(transaction.get("creditPaid", None))
            dailyTransactionState["dutchie_transaction_gift_paid"].append(transaction.get("giftPaid", None))
            dailyTransactionState["dutchie_transaction_mmap_paid"].append(transaction.get("mmapPaid", None))
            dailyTransactionState["dutchie_transaction_pre_payment_amount"].append(transaction.get("prePaymentAmount", None))
            dailyTransactionState["dutchie_transaction_revenue_fees_and_donations"].append(transaction.get("revenueFeesAndDonations", None))
            dailyTransactionState["dutchie_transaction_non_revenue_fees_and_donations"].append(transaction.get("nonRevenueFeesAndDonations", None))
            dailyTransactionState["dutchie_transaction_fees_and_donations"].append(transaction.get("feesAndDonations", None))
            dailyTransactionState["dutchie_transaction_tax_summary"].append(transaction.get("taxSummary", None))
            dailyTransactionState["dutchie_transaction_return_on_transaction_id"].append(transaction.get("returnOnTransactionId", None))
            dailyTransactionState["dutchie_transaction_adjustment_for_transaction_id"].append(transaction.get("adjustmentForTransactionId", None))
            dailyTransactionState["dutchie_transaction_order_type"].append(transaction.get("orderType", None))
            dailyTransactionState["dutchie_transaction_was_pre_ordered"].append(transaction.get("wasPreOrdered", None))
            dailyTransactionState["dutchie_transaction_order_source"].append(transaction.get("orderSource", None))
            dailyTransactionState["dutchie_transaction_order_method"].append(transaction.get("orderMethod", None))
            dailyTransactionState["dutchie_transaction_invoice_name"].append(transaction.get("invoiceName", None))
            dailyTransactionState["dutchie_transaction_is_return"].append(transaction.get("isReturn", None))
            dailyTransactionState["dutchie_transaction_auth_code"].append(transaction.get("authCode", None))
            dailyTransactionState["dutchie_transaction_customer_type_id"].append(transaction.get("customerTypeId", None))
            dailyTransactionState["dutchie_transaction_is_medical"].append(transaction.get("isMedical", None))
            dailyTransactionState["dutchie_transaction_order_ids"].append(transaction.get("orderIds", None))
            dailyTransactionState["dutchie_transaction_total_credit"].append(transaction.get("totalCredit", None))
            dailyTransactionState["dutchie_transaction_completed_by_user"].append(transaction.get("completedByUser", None))
            dailyTransactionState["dutchie_transaction_date_local_time"].append(transaction.get("transactionDateLocalTime", None))
            dailyTransactionState["dutchie_transaction_est_time_arrival_local"].append(transaction.get("estTimeArrivalLocal", None))
            dailyTransactionState["dutchie_transaction_est_delivery_date_local"].append(transaction.get("estDeliveryDateLocal", None))
            integratedPayments = transaction.get("integratedPayments", None)
            if integratedPayments != []:
                dailyTransactionState["dutchie_transaction_integrationType"].append(integratedPayments[0]["integrationType"])
                dailyTransactionState["dutchie_transaction_integratedPaid"].append(integratedPayments[0]["integratedPaid"])
                dailyTransactionState["dutchie_transaction_externalPaymentId"].append(integratedPayments[0]["externalPaymentId"])
            else:
                dailyTransactionState["dutchie_transaction_integrationType"].append(None)
                dailyTransactionState["dutchie_transaction_integratedPaid"].append(None)
                dailyTransactionState["dutchie_transaction_externalPaymentId"].append(None)

            for item in items:
                dailyTransactionItemState["master_code"].append(code)
                dailyTransactionItemState["master_state"].append(master_state)
                dailyTransactionItemState["master_store_name"].append(location_name)
                dailyTransactionItemState["master_customer_id"].append(transaction.get("customerId", None))
                dailyTransactionItemState["dutchie_transaction_id"].append(str(item.get("transactionId", None)))
                dailyTransactionItemState["dutchie_transaction_item_id"].append(item.get("transactionItemId", None))
                dailyTransactionItemState["dutchie_transaction_item_batch_name"].append(item.get("batchName", None))
                dailyTransactionItemState["dutchie_transaction_item_flower_equivalent"].append(item.get("flowerEquivalent", None))
                dailyTransactionItemState["dutchie_transaction_item_flower_equivalent_unit"].append(item.get("flowerEquivalentUnit", None))
                dailyTransactionItemState["dutchie_transaction_item_is_coupon"].append(item.get("isCoupon", None))
                dailyTransactionItemState["dutchie_transaction_item_is_returned"].append(item.get("isReturned", None))
                dailyTransactionItemState["dutchie_transaction_item_package_id"].append(str(item.get("packageId", None)))
                dailyTransactionItemState["dutchie_transaction_item_product_id"].append(item.get("productId", None))
                dailyTransactionItemState["dutchie_transaction_item_quantity"].append(item.get("quantity", None))
                dailyTransactionItemState["dutchie_transaction_item_return_date"].append(item.get("returnDate", None))
                dailyTransactionItemState["dutchie_transaction_item_return_reason"].append(item.get("returnReason", None))
                dailyTransactionItemState["dutchie_transaction_item_returned_by_transaction_id"].append(item.get("returnedByTransactionId", None))
                dailyTransactionItemState["dutchie_transaction_item_source_package_id"].append(item.get("sourcePackageId", None))
                dailyTransactionItemState["dutchie_transaction_item_taxes"].append(item.get("taxes", None))
                dailyTransactionItemState["dutchie_transaction_item_total_discount"].append(item.get("totalDiscount", None))
                dailyTransactionItemState["dutchie_transaction_item_total_price"].append(item.get("totalPrice", None))
                dailyTransactionItemState["dutchie_transaction_item_unit_cost"].append(item.get("unitCost", None))
                dailyTransactionItemState["dutchie_transaction_item_unit_id"].append(item.get("unitId", None))
                dailyTransactionItemState["dutchie_transaction_item_unit_price"].append(item.get("unitPrice", None))
                dailyTransactionItemState["dutchie_transaction_item_unit_weight"].append(item.get("unitWeight", None))
                dailyTransactionItemState["dutchie_transaction_item_unit_weight_unit"].append(item.get("unitWeightUnit", None))
                dailyTransactionItemState["dutchie_transaction_item_vendor"].append(item.get("vendor", None))
                discounts = item.get("discounts", None)
                if discounts is not None:
                    for discount in discounts:
                        dailyTransactionsDiscountState["master_code"].append(code)
                        dailyTransactionsDiscountState["master_state"].append(master_state)
                        dailyTransactionsDiscountState["master_store_name"].append(location_name)
                        dailyTransactionsDiscountState["master_customer_id"].append(transaction.get("customerId", None))
                        dailyTransactionsDiscountState["dutchie_transaction_discount_id"].append(str(discount.get("discountId", None)))
                        dailyTransactionsDiscountState["dutchie_transaction_discount_transaction_id"].append(str(transaction.get("transactionId", None)))
                        dailyTransactionsDiscountState["dutchie_transaction_discount_amount"].append(discount.get("amount", None))
                        dailyTransactionsDiscountState["dutchie_transaction_discount_name"].append(discount.get("discountName", None))
                        dailyTransactionsDiscountState["dutchie_transaction_discount_reason"].append(discount.get("discountReason", None))
                        dailyTransactionsDiscountState["dutchie_transaction_discount_transaction_item_id"].append(str(discount.get("transactionItemId", None)))

    if start_date_dt < today_dt:
        start_date = end_date
    else:
        hasMore = False

    pd.DataFrame(dailyTransactionState).to_csv("workaxle/Data/main_raw/massachusetts/main_raw_dutchie_transactions.csv", date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False, mode="a", header=False)
    pd.DataFrame(dailyTransactionItemState).to_csv("workaxle/Data/main_raw/massachusetts/main_raw_dutchie_transaction_items.csv", date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False, mode="a", header=False)
    pd.DataFrame(dailyTransactionsDiscountState).to_csv("workaxle/Data/main_raw/massachusetts/main_raw_dutchie_transaction_discounts.csv", date_format='%Y-%m-%d %H:%M:%S', float_format='%.2f', index=False, mode="a", header=False)
