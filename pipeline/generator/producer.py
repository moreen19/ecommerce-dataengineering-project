# Purpose: Produces products, streaming sales transactions,
# and restocking activities to Confluent Cloud Kafka

import configparser
import json
import random
import time
from csv import reader
from datetime import datetime

from confluent_kafka import Producer


from generator.config.kafka import get_configs, delivery_report, get_raw_config, KAFKA_SETTINGS
from generator.models.product import Product
from generator.models.purchase import Purchase
from generator.models.inventory import Inventory

config = get_raw_config()


# =========================
# RATE / COST CONTROL
# =========================
MAX_DURATION_SECONDS = 120        # hard stop
MIN_SLEEP_SECONDS = 0.45
MAX_SLEEP_SECONDS = 0.55          # ~2 events/sec


# =========================
# LOAD CONFIG
# =========================


# Kafka topics
topic_products = KAFKA_SETTINGS["topic_products"]
topic_purchases = KAFKA_SETTINGS["topic_purchases"]
topic_inventories = KAFKA_SETTINGS["topic_inventories"]

# Sales configuration
transaction_quantity_one_item_freq = int(
    config["SALES"]["transaction_quantity_one_item_freq"]
)
item_quantity_one_freq = int(config["SALES"]["item_quantity_one_freq"])
member_freq = int(config["SALES"]["member_freq"])
club_member_discount = float(config["SALES"]["club_member_discount"])
add_supp_freq_group1 = int(config["SALES"]["add_supp_freq_group1"])
add_supp_freq_group2 = int(config["SALES"]["add_supp_freq_group2"])
supplements_cost = float(config["SALES"]["supplements_cost"])

# Inventory configuration
min_inventory = int(config["INVENTORY"]["min_inventory"])
restock_amount = int(config["INVENTORY"]["restock_amount"])

# =========================
# GLOBAL STATE
# =========================
products = []
propensity_to_buy_range = []
producer = None


# =========================
# MAIN
# =========================
def main():
    global producer

    producer = Producer(get_configs())
    print("Connected to Confluent Cloud Kafka")

    create_product_list()
    generate_sales()

    print("Flushing final Kafka messages...")
    producer.flush()


# =========================
# PRODUCT LOAD
# =========================
def create_product_list():
    with open("/opt/airflow/generator/data/products.csv", "r") as csv_file:
        next(csv_file)  # skip header
        rows = list(reader(csv_file))




    for p in rows:
        product = Product(
            str(datetime.utcnow()),
            p[0],   # product_id
            p[1],   # category
            p[2],   # item
            p[3],   # size
            p[4],   # cogs
            p[5],   # price
            p[6],   # inventory_level
            to_bool(p[7]),
            to_bool(p[8]),
            to_bool(p[9]),
            to_bool(p[10]),
            p[14],  # propensity_to_buy
        )

        products.append(product)
        propensity_to_buy_range.append(product.propensity_to_buy)

        publish_to_kafka(topic_products, product)

    propensity_to_buy_range.sort()
    print(f"Loaded {len(products)} products")


# =========================
# SALES GENERATION
# =========================
def generate_sales():
    print("Starting streaming sales generator...")
    end_time = time.time() + MAX_DURATION_SECONDS

    range_min = propensity_to_buy_range[0]
    range_max = propensity_to_buy_range[-1]

    while time.time() < end_time:
        transaction_time = str(datetime.utcnow())
        is_member = random_club_member()
        member_discount = club_member_discount if is_member else 0.0

        rnd_propensity = closest_product_match(
            propensity_to_buy_range,
            random.randint(range_min, range_max),
        )

        quantity = random_quantity()

        for p in products:
            if p.propensity_to_buy == rnd_propensity:
                add_supplements = random_add_supplements(p.product_id)
                supplement_price = supplements_cost if add_supplements else 0.0

                purchase = Purchase(
                    transaction_time,
                    str(abs(hash(transaction_time))),
                    p.product_id,
                    p.price,
                    quantity,
                    is_member,
                    member_discount,
                    add_supplements,
                    supplement_price,
                )

                publish_to_kafka(topic_purchases, purchase)

                p.inventory_level -= quantity
                if p.inventory_level <= min_inventory:
                    restock_item(p)

                break

        time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))


# =========================
# INVENTORY RESTOCK
# =========================
def restock_item(product):
    new_level = product.inventory_level + restock_amount

    inventory = Inventory(
        str(datetime.utcnow()),
        product.product_id,
        product.inventory_level,
        restock_amount,
        new_level,
    )

    product.inventory_level = new_level
    publish_to_kafka(topic_inventories, inventory)


# =========================
# KAFKA PRODUCE
# =========================
def publish_to_kafka(topic, message):
    producer.produce(
        topic=topic,
        value=json.dumps(vars(message)).encode("utf-8"),
        callback=delivery_report,
    )
    producer.poll(0)


# =========================
# HELPERS
# =========================
def to_bool(value):
    return str(value).lower() == "true"


def closest_product_match(lst, k):
    return lst[min(range(len(lst)), key=lambda i: abs(lst[i] - k))]


def random_quantity():
    rnd = random.randint(1, 30)
    if rnd == 30:
        return 3
    if rnd <= item_quantity_one_freq:
        return 1
    return 2


def random_club_member():
    return random.randint(1, 10) <= member_freq


def random_add_supplements(product_id):
    rnd = random.randint(1, 10)
    if str(product_id).startswith(("SF", "SC")):
        return rnd <= add_supp_freq_group1
    return rnd <= add_supp_freq_group2


# =========================
if __name__ == "__main__":
    main()
