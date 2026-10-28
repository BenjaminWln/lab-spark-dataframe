#!/usr/bin/env python3
"""Fallback generator for the bronze datasets (users.csv / orders.csv).

It follows the `dataset-users` and `dataset-orders` logic of the uv lab. The generated order count
can vary with the installed Faker version.
Use it only if you do not have the generators of the uv lab (or copy your own CSV files into
data/bronze/ instead).

  users.csv  : uuid, username, name, sex, address (2 lines), mail, birthdate
  orders.csv : uuid, user_uuid, date, quantity, product

Usage:
  python scripts/generate_datasets.py --out data/bronze
"""
import argparse
import csv
import datetime
from pathlib import Path

from faker import Faker
from faker.providers import DynamicProvider

PRODUCTS = ["bread", "brioche", "cookie", "croissant", "donut", "drink"]


def write_csv(path, records):
    # Same format as the lab's serialize(): csv.DictWriter (rows end with \r\n)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)


def users_generate(fake, count):
    return [{"uuid": fake.uuid4(), **fake.simple_profile()} for _ in range(count)]


def orders_generate(fake, count_min, count_max, count_users, date_from):
    orders = []
    date_start = date_from
    # Like `dataset-orders`: the users are generated first, then each one places some orders
    for user in users_generate(fake, count_users):
        for _ in range(fake.pyint(min_value=count_min, max_value=count_max)):
            orders.append(
                {
                    "uuid": fake.uuid4(),
                    "user_uuid": user["uuid"],
                    "date": fake.date_time_between(
                        date_start, date_start + datetime.timedelta(hours=1), tzinfo=datetime.UTC
                    ),
                    "quantity": fake.pyint(min_value=1, max_value=5),
                    "product": fake.product(),
                }
            )
            # Each order is placed in the hour following the previous one
            date_start += datetime.timedelta(hours=1)
    return orders


def main():
    parser = argparse.ArgumentParser(description="Bronze datasets generator (users and orders)")
    parser.add_argument("--out", default="data/bronze", help="Output directory.")
    parser.add_argument("-u", "--users", type=int, default=50, help="Number of users.")
    parser.add_argument("-C", "--count-min", type=int, default=0, help="Min orders per user.")
    parser.add_argument("-c", "--count-max", type=int, default=100, help="Max orders per user.")
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    date_from = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC)

    # users.csv: same as `dataset-users` (seed 42)
    Faker.seed(42)
    fake = Faker()
    fake.add_provider(DynamicProvider(provider_name="product", elements=PRODUCTS))
    write_csv(out / "users.csv", users_generate(fake, args.users))

    # orders.csv: same as `dataset-orders`, which restarts from the same seed
    Faker.seed(42)
    orders = orders_generate(fake, args.count_min, args.count_max, args.users, date_from)
    write_csv(out / "orders.csv", orders)
    print(f"Wrote {out / 'users.csv'} ({args.users} users) and {out / 'orders.csv'} ({len(orders)} orders)")


if __name__ == "__main__":
    main()
