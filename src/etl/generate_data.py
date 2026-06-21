"""Generate synthetic raw datasets for DataPulse AI pipeline."""
import os, sys, csv, random, uuid
from datetime import datetime, timedelta
from faker import Faker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from config.settings import RAW_DIR

fake = Faker()
Faker.seed(42)
random.seed(42)

PRODUCTS = [
    ("PRD-001", "Wireless Mouse", "Electronics", "Peripherals", 29.99),
    ("PRD-002", "Mechanical Keyboard", "Electronics", "Peripherals", 89.99),
    ("PRD-003", "USB-C Hub", "Electronics", "Accessories", 49.99),
    ("PRD-004", "27-inch Monitor", "Electronics", "Displays", 349.99),
    ("PRD-005", "Laptop Stand", "Office", "Ergonomics", 45.00),
    ("PRD-006", "Webcam HD", "Electronics", "Peripherals", 79.99),
    ("PRD-007", "Noise-Cancel Headphones", "Electronics", "Audio", 199.99),
    ("PRD-008", "Desk Lamp LED", "Office", "Lighting", 34.99),
    ("PRD-009", "Ergonomic Chair", "Office", "Furniture", 499.99),
    ("PRD-010", "Standing Desk", "Office", "Furniture", 699.99),
    ("PRD-011", "Portable SSD 1TB", "Electronics", "Storage", 109.99),
    ("PRD-012", "Bluetooth Speaker", "Electronics", "Audio", 59.99),
    ("PRD-013", "Cable Management Kit", "Office", "Accessories", 19.99),
    ("PRD-014", "Whiteboard 4x3", "Office", "Supplies", 89.99),
    ("PRD-015", "Smart Power Strip", "Electronics", "Accessories", 39.99),
]

REGIONS = ["US-East", "US-West", "US-Central", "EU-West", "EU-East", "APAC"]
CHANNELS = ["web", "mobile", "in-store", "marketplace", "api"]
STATUSES = ["completed", "pending", "shipped", "cancelled", "returned", "processing"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto", "gift_card"]
PAYMENT_STATUSES = ["success", "failed", "pending", "refunded"]
EVENT_TYPES = ["page_view", "product_click", "add_to_cart", "remove_from_cart", "checkout_start", "purchase", "search"]
DEVICES = ["desktop", "mobile", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
SUB_PLANS = ["basic", "pro", "enterprise", "starter"]
SUB_EVENTS = ["created", "renewed", "upgraded", "downgraded", "cancelled", "paused"]
API_ENDPOINTS = ["/api/orders", "/api/products", "/api/customers", "/api/inventory", "/api/payments", "/api/search", "/api/reports"]
HTTP_METHODS = ["GET", "POST", "PUT", "DELETE"]

NUM_CUSTOMERS = 500
NUM_ORDERS = 2000
NUM_PAYMENTS = 1800
NUM_INVENTORY = 300
NUM_CLICKSTREAM = 5000
NUM_SUBSCRIPTIONS = 400
NUM_API_LOGS = 3000


def _write_csv(filename, headers, rows):
    path = os.path.join(RAW_DIR, filename)
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  Generated {path} ({len(rows)} rows)")
    return path


def generate_customers():
    rows = []
    for i in range(NUM_CUSTOMERS):
        cid = f"CUST-{i+1:04d}"
        row = {
            "customer_id": cid,
            "name": fake.name(),
            "email": fake.email() if random.random() > 0.03 else "",
            "phone": fake.phone_number() if random.random() > 0.05 else None,
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": random.choice(["US", "US", "US", "CA", "UK", "DE", "IN"]),
            "signup_date": fake.date_time_between(start_date="-3y", end_date="now").isoformat(),
            "customer_type": random.choice(["individual", "business", "enterprise"]),
        }
        rows.append(row)

    # inject duplicates for DQ testing
    for _ in range(10):
        dup = rows[random.randint(0, len(rows)-1)].copy()
        rows.append(dup)

    random.shuffle(rows)
    return _write_csv("raw_customers.csv", list(rows[0].keys()), rows)


def generate_orders():
    rows = []
    for i in range(NUM_ORDERS):
        prod = random.choice(PRODUCTS)
        qty = random.randint(1, 20)
        price = prod[4]
        total = round(qty * price, 2)

        # inject bad data: negative qty, null order_id
        if random.random() < 0.02:
            qty = -random.randint(1, 5)
            total = round(qty * price, 2)

        oid = f"ORD-{i+1:06d}" if random.random() > 0.01 else ""

        row = {
            "order_id": oid,
            "customer_id": f"CUST-{random.randint(1, NUM_CUSTOMERS):04d}",
            "product_id": prod[0],
            "order_date": fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
            "quantity": qty,
            "unit_price": price,
            "total_amount": total,
            "status": random.choice(STATUSES),
            "channel": random.choice(CHANNELS),
            "region": random.choice(REGIONS),
        }
        rows.append(row)

    # inject duplicates
    for _ in range(15):
        dup = rows[random.randint(0, len(rows)-1)].copy()
        rows.append(dup)

    return _write_csv("raw_orders.csv", list(rows[0].keys()), rows)


def generate_payments():
    rows = []
    for i in range(NUM_PAYMENTS):
        row = {
            "payment_id": f"PAY-{i+1:06d}",
            "order_id": f"ORD-{random.randint(1, NUM_ORDERS):06d}",
            "payment_date": fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
            "amount": round(random.uniform(10, 5000), 2) if random.random() > 0.02 else -round(random.uniform(10, 100), 2),
            "method": random.choice(PAYMENT_METHODS),
            "status": random.choice(PAYMENT_STATUSES),
            "currency": random.choice(["USD", "USD", "USD", "EUR", "GBP", "CAD"]),
        }
        rows.append(row)
    return _write_csv("raw_payments.csv", list(rows[0].keys()), rows)


def generate_inventory():
    rows = []
    warehouses = [f"WH-{j+1:03d}" for j in range(10)]
    for i in range(NUM_INVENTORY):
        prod = random.choice(PRODUCTS)
        row = {
            "product_id": prod[0],
            "warehouse_id": random.choice(warehouses),
            "warehouse_region": random.choice(REGIONS) if random.random() > 0.05 else "",
            "quantity_on_hand": random.randint(-5, 500),
            "reorder_level": random.randint(10, 100),
            "last_restock_date": fake.date_time_between(start_date="-6m", end_date="now").isoformat(),
            "snapshot_date": datetime.now().isoformat(),
        }
        rows.append(row)
    return _write_csv("raw_inventory.csv", list(rows[0].keys()), rows)


def generate_clickstream():
    rows = []
    pages = ["/home", "/products", "/product/detail", "/cart", "/checkout", "/account", "/search", "/support"]
    for i in range(NUM_CLICKSTREAM):
        row = {
            "event_id": f"EVT-{i+1:07d}",
            "session_id": f"SES-{random.randint(1,1000):05d}",
            "customer_id": f"CUST-{random.randint(1, NUM_CUSTOMERS):04d}" if random.random() > 0.2 else "",
            "event_type": random.choice(EVENT_TYPES),
            "page_url": random.choice(pages),
            "referrer": random.choice(["google.com", "facebook.com", "direct", "email", "twitter.com", ""]),
            "device_type": random.choice(DEVICES),
            "browser": random.choice(BROWSERS),
            "event_timestamp": fake.date_time_between(start_date="-30d", end_date="now").isoformat(),
            "duration_seconds": random.randint(0, 600) if random.random() > 0.05 else -1,
        }
        rows.append(row)
    return _write_csv("raw_clickstream_events.csv", list(rows[0].keys()), rows)


def generate_subscriptions():
    rows = []
    for i in range(NUM_SUBSCRIPTIONS):
        plan = random.choice(SUB_PLANS)
        amounts = {"basic": 9.99, "starter": 19.99, "pro": 49.99, "enterprise": 199.99}
        row = {
            "subscription_id": f"SUB-{i+1:05d}",
            "customer_id": f"CUST-{random.randint(1, NUM_CUSTOMERS):04d}",
            "plan": plan,
            "event_type": random.choice(SUB_EVENTS),
            "event_date": fake.date_time_between(start_date="-1y", end_date="now").isoformat(),
            "monthly_amount": amounts[plan],
            "billing_cycle": random.choice(["monthly", "annual"]),
        }
        rows.append(row)
    return _write_csv("raw_subscription_events.csv", list(rows[0].keys()), rows)


def generate_api_logs():
    rows = []
    for i in range(NUM_API_LOGS):
        sc = random.choices([200, 201, 400, 401, 403, 404, 500, 502, 503], weights=[50, 10, 8, 5, 3, 8, 6, 3, 7])[0]
        row = {
            "request_id": f"REQ-{uuid.uuid4().hex[:12]}",
            "endpoint": random.choice(API_ENDPOINTS),
            "method": random.choice(HTTP_METHODS),
            "status_code": sc,
            "response_time_ms": random.randint(5, 5000),
            "client_ip": fake.ipv4(),
            "user_agent": fake.user_agent(),
            "request_timestamp": fake.date_time_between(start_date="-7d", end_date="now").isoformat(),
            "error_message": fake.sentence() if sc >= 400 else "",
        }
        rows.append(row)
    return _write_csv("raw_api_logs.csv", list(rows[0].keys()), rows)


def generate_products():
    rows = []
    for p in PRODUCTS:
        rows.append({
            "product_id": p[0],
            "product_name": p[1],
            "category": p[2],
            "subcategory": p[3],
            "unit_price": p[4],
        })
    return _write_csv("raw_products.csv", list(rows[0].keys()), rows)


def generate_all():
    os.makedirs(RAW_DIR, exist_ok=True)
    print("Generating synthetic datasets...")
    generate_products()
    generate_customers()
    generate_orders()
    generate_payments()
    generate_inventory()
    generate_clickstream()
    generate_subscriptions()
    generate_api_logs()
    print("All datasets generated.")


if __name__ == "__main__":
    generate_all()
