import json
import logging

import redis

from allocation import config
from allocation.adapters import orm
from allocation.domain import commands
from allocation.service_layer import messagebus, unit_of_work

logger = logging.getLogger(__name__)

r = redis.Redis(**config.get_redis_host_and_port())


def is_change_batch_quantity(m):
    return m["channel"] == "change_batch_quantity"


def is_allocate(m):
    return m["channel"] == "allocate"


def main():
    logging.info("Starting Redis event consumer")
    orm.start_mappers()
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe("change_batch_quantity")
    pubsub.subscribe("allocate")
    logging.info("Subscribed to channels - change_batch_quantity, allocate")

    for m in pubsub.listen():
        if is_change_batch_quantity(m):
            handle_change_batch_quantity(m)
        elif is_allocate(m):
            handle_allocate(m)


def handle_change_batch_quantity(m):
    logging.info("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.ChangeBatchQuantity(ref=data["batchref"], qty=data["qty"])
    messagebus.handle(cmd, uow=unit_of_work.SqlAlchemyUnitOfWork())


def handle_allocate(m):
    logging.info("handling %s", m)
    data = json.loads(m["data"])
    cmd = commands.Allocate(orderid=data["orderid"], sku=data["sku"], qty=data["qty"])
    messagebus.handle(cmd, uow=unit_of_work.SqlAlchemyUnitOfWork())


if __name__ == "__main__":
    print("Starting Redis event consumer")
    main()
