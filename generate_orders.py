import pandas as pd
from faker import Faker
import random

fake = Faker('en_GB')

products = [
    'Laptop', 'Phone', 'Headphones', 'Keyboard',
    'Monitor', 'Mouse', 'Webcam', 'Charger', 'Tablet', 'Speaker'
]

statuses = ['completed', 'cancelled', 'pending', 'refunded']

def generate_orders(n=10000):
    orders = []
    for i in range(1, n + 1):
        orders.append({
            'order_id': f'ORD-{i:05d}',
            'customer_name': fake.name(),
            'email': fake.email(),
            'product': random.choice(products),
            'quantity': random.randint(1, 5),
            'unit_price': round(random.uniform(9.99, 999.99), 2),
            'order_date': fake.date_between(
                start_date='-1y', end_date='today'
            ).strftime('%Y-%m-%d'),
            'status': random.choices(
                statuses, weights=[70, 10, 15, 5]
            )[0],
            'city': fake.city(),
            'country': 'UK'
        })
    return pd.DataFrame(orders)

df = generate_orders()
df['total_revenue'] = df['quantity'] * df['unit_price']
df.to_csv('data/orders_raw.csv', index=False)
print(f"Generated {len(df)} orders")
print(df.head())