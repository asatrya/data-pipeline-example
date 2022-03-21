from fastapi import FastAPI
from urllib.parse import unquote
from datetime import datetime, date
from mysql.connector import connect, Error
import json


app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World!!"}


def __get_recency_segment(last_order_ts: datetime) -> str:
    # calculate days since last order
    days_since_last_order = (date.today() - last_order_ts.date()).days

    # classify the segment
    if days_since_last_order <= 30:
        segment = '0-30'
    elif days_since_last_order > 30 and days_since_last_order <= 60:
        segment = '30-60'
    elif days_since_last_order > 60 and days_since_last_order <= 90:
        segment = '60-90'
    elif days_since_last_order > 90 and days_since_last_order <= 120:
        segment = '90-120'
    elif days_since_last_order > 120 and days_since_last_order <= 180:
        segment = '120-180'
    elif days_since_last_order > 180:
        segment = '180+'

    return segment


def __get_frequent_segment(total_orders: int) -> str:
    if total_orders <= 4:
        segment = '0-4'
    elif total_orders > 4 and total_orders <= 13:
        segment = '5-13'
    elif total_orders > 13 and total_orders <= 37:
        segment = '13-37'
    elif total_orders > 37:
        segment = '37+'
    
    return segment


def __read_voucher_recency_most_used(country_code: str, segment: str) -> int:
    with connect(host="mysql", user='user', password='password', database='db') as connection:
        query = """SELECT voucher_amount 
                    FROM voucher_recency_most_used 
                    WHERE country_code='{}' 
                    AND recency_segment='{}' 
                    LIMIT 1""".format(country_code, segment)
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            voucher_amount = result[0] if result is not None else result

    return voucher_amount


def __read_voucher_frequent_most_used(country_code: str, segment: str) -> int:
    with connect(host="mysql", user='user', password='password', database='db') as connection:
        query = """SELECT voucher_amount 
                    FROM voucher_frequent_most_used 
                    WHERE country_code='{}' 
                    AND frequent_segment='{}' 
                    LIMIT 1""".format(country_code, segment)
        
        with connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
            voucher_amount = result[0] if result is not None else result

    return voucher_amount


@app.get("/voucher/most-used")
def read_voucher_most_used(customer: str):
    # get parameter values
    customer_str = unquote(customer)
    customer_obj = json.loads(customer_str)

    # populate customer properties
    customer_id = str(customer_obj['customer_id'])
    country_code = str(customer_obj['country_code'])
    segment_name = str(customer_obj['segment_name'])
    total_orders = int(customer_obj['total_orders'])
    last_order_ts = datetime.strptime(customer_obj['last_order_ts'], '%Y-%m-%d %H:%M:%S')
    first_order_ts = datetime.strptime(customer_obj['first_order_ts'], '%Y-%m-%d %H:%M:%S')

    # segment classification
    if segment_name == 'recency_segment':
        segment = __get_recency_segment(last_order_ts)
        
        # get most used voucher amount
        voucher_amount = __read_voucher_recency_most_used(country_code, segment)
    
    elif segment_name == 'frequent_segment':
        segment = __get_frequent_segment(total_orders)

        # get most used voucher amount
        voucher_amount = __read_voucher_frequent_most_used(country_code, segment)

    # return value
    return {"voucher_amount": voucher_amount}
