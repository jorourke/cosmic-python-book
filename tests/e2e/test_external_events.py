import json

import pytest
from tenacity import Retrying, stop_after_delay

from ..random_refs import random_batchref, random_orderid, random_sku
from . import api_client, redis_client


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
@pytest.mark.usefixtures("restart_redis_pubsub")
def test_change_batch_quantity_leading_to_reallocation():
    # start with two batches and an order allocated to one of them
    orderid, sku = random_orderid(), random_sku()
    earlier_batch, later_batch = random_batchref("old"), random_batchref("newer")
    api_client.post_to_add_batch(earlier_batch, sku, qty=10, eta="2011-01-01")
    api_client.post_to_add_batch(later_batch, sku, qty=10, eta="2011-01-02")
    response = api_client.post_to_allocate(orderid, sku, 10)
    assert response.json()["batchref"] == earlier_batch

    subscription = redis_client.subscribe_to("line_allocated")

    # change quantity on allocated batch so it's less than our order
    redis_client.publish_message(
        "change_batch_quantity",
        {"batchref": earlier_batch, "qty": 5},
    )

    # wait until we see a message saying the order has been reallocated
    messages = []
    for attempt in Retrying(stop=stop_after_delay(3), reraise=True):
        with attempt:
            message = subscription.get_message(timeout=1)
            print(f"Message: {message}")
            if message and message["type"] == "message":
                messages.append(message)
            if messages:  # Only try to process messages if we have any
                data = json.loads(messages[-1]["data"])
                assert data["orderid"] == orderid
                assert data["batchref"] == later_batch


@pytest.mark.usefixtures("postgres_db")
@pytest.mark.usefixtures("restart_api")
@pytest.mark.usefixtures("restart_redis_pubsub")
def test_allocate_message():
    orderid, sku = random_orderid(), random_sku()
    batch = random_batchref()

    # First, create a batch that we can allocate to
    api_client.post_to_add_batch(batch, sku, qty=10, eta="2011-01-01")

    # Subscribe to the line_allocated channel to see the result
    subscription = redis_client.subscribe_to("line_allocated")

    # Publish the allocate command
    redis_client.publish_message(
        "allocate", {"orderid": orderid, "sku": sku, "qty": 10}
    )

    # Wait for and verify the allocation result
    messages = []
    for attempt in Retrying(stop=stop_after_delay(3), reraise=True):
        with attempt:
            message = subscription.get_message(timeout=1)
            print(f"Message: {message}")
            if message and message["type"] == "message":
                messages.append(message)
            if messages:
                data = json.loads(messages[-1]["data"])
                assert data["orderid"] == orderid
                assert data["batchref"] == batch
