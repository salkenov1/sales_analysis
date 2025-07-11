import json
from clickhouse_connect import get_client
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLICKHOUSE_HOST = 'localhost'
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = 'admin'
CLICKHOUSE_PASSWORD = 'secret123'
DATABASE = 'orders'
TABLE_NAME = 'raw_data'
JSONL_FILE = 'mega_orders_10M.jsonl'

# Connect to ClickHouse
client = get_client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    username=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=DATABASE
)

# Create table if not exists
client.command(f'''
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    order_id UInt32,
    user_id UInt32,
    user_name String,
    user_email String,
    user_country String,
    order_date DateTime64(0),
    item_id UInt32,
    item_name String,
    item_category String,
    item_quantity UInt64,
    item_price Float64,
    payment_id UInt32,
    payment_method String,
    payment_status String,
    payment_amount Float64,
    payment_currency String
) 
ENGINE = MergeTree()
ORDER BY order_id
''')

# Load data
with open(JSONL_FILE, 'r', encoding='utf-8') as f:
    raw_rows = [json.loads(line.strip()) for line in f if line.strip()]

flattened_rows = []

for order in raw_rows:
    order_id = int(order['order_id'].split('_')[1])
    order_date = datetime.fromisoformat(order['order_date'])
    if 'user' not in order:
        print(f"Missing user in order: {order.get('order_id')}")
    else:
        user = order.get('user', {})
        user_id = int(user['user_id'].split('_')[1])
        user_name = user['name']
        user_email = user['email']
        user_country = user['country']
    
    payment = order.get('payment', {})
    payment_id = int(payment['payment_id'].split('_')[1])
    payment_method = payment['method']
    payment_status = payment['status']
    payment_amount = float(payment['amount'])
    payment_currency = payment['currency']
    
    for item in order['items']:
        item_id = int(item['item_id'].split('_')[1])
        item_name = item['name']
        item_category = item['category']
        quantity = int(item['quantity'])
        price = float(item['price'])

        flattened_rows.append({
            "order_id": order_id,
            "user_id": user_id,
            "user_name": user_name,
            "user_email": user_email,
            "user_country": user_country,
            "order_date": order_date,
            "item_id": item_id,
            "item_name": item_name,
            "item_category": item_category,
            "item_quantity": quantity,
            "item_price": price,
            "payment_id": payment_id,
            "payment_method": payment_method,
            "payment_status": payment_status,
            "payment_amount": payment_amount,
            "payment_currency": payment_currency
        })

print(len(flattened_rows))

columns = [
    'order_id',
    'user_id',
    'user_name',
    'user_email',
    'user_country',
    'order_date',
    'item_id',
    'item_name',
    'item_category',
    'item_quantity',
    'item_price',
    'payment_id',
    'payment_method',
    'payment_status',
    'payment_amount',
    'payment_currency'
]

data_to_insert = [tuple(row[col] for col in columns) for row in flattened_rows]

client.insert('orders.raw_data', data_to_insert, column_names=columns)