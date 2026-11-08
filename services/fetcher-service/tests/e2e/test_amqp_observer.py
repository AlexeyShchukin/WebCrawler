"""The acceptance observer must correlate actual ACKs without changing frames."""

from pamqp import body, frame, header
from pamqp.commands import Basic

from .amqp_observer import AMQPObserver


def delivered(tag, channel=1, *, redelivered=False, payload=b'{"crawl_id":"crawl-a"}'):
    return b"".join([
        frame.marshal(Basic.Deliver(
            consumer_tag="test", delivery_tag=tag, redelivered=redelivered,
            exchange="crawler.topic", routing_key="fetch.url"), channel),
        frame.marshal(header.ContentHeader(body_size=len(payload)), channel),
        frame.marshal(body.ContentBody(payload[:4]), channel),
        frame.marshal(body.ContentBody(payload[4:]), channel),
    ])


def test_fragmented_delivery_correlates_only_outbound_ack():
    records = []
    observer = AMQPObserver(records)
    for byte in frame.marshal(header.ProtocolHeader(), 0):
        observer.feed(bytes([byte]), from_broker=False)
    for byte in delivered(7, redelivered=True):
        observer.feed(bytes([byte]), from_broker=True)
    assert len(records) == 1
    assert records[0]["payload"] == {"crawl_id": "crawl-a"}
    assert records[0]["redelivered"] is True
    assert records[0]["ack_count"] == 0
    # Broker publisher-confirm tags overlap delivery tags; never count them.
    ack = frame.marshal(Basic.Ack(delivery_tag=7), 1)
    observer.feed(ack, from_broker=True)
    assert records[0]["ack_count"] == 0
    for byte in ack:
        observer.feed(bytes([byte]), from_broker=False)
    assert records[0]["ack_count"] == 1
    assert records[0]["settlement"] == "ack"


def test_multiple_ack_and_connections_preserve_delivery_identity():
    records = []
    first, second = AMQPObserver(records), AMQPObserver(records)
    first.feed(delivered(1) + delivered(2) + delivered(1, channel=2), from_broker=True)
    second.feed(delivered(1), from_broker=True)
    first.feed(frame.marshal(Basic.Ack(delivery_tag=2, multiple=True), 1), from_broker=False)
    assert [item["ack_count"] for item in records] == [1, 1, 0, 0]
    second.feed(frame.marshal(Basic.Ack(delivery_tag=0, multiple=True), 1), from_broker=False)
    assert [item["ack_count"] for item in records] == [1, 1, 0, 1]
    assert records[0]["connection_id"] != records[-1]["connection_id"]


def test_reject_is_not_mistaken_for_ack_and_invalid_body_does_not_break_observation():
    records = []
    observer = AMQPObserver(records)
    observer.feed(delivered(1, payload=b"invalid body"), from_broker=True)
    observer.feed(frame.marshal(Basic.Reject(delivery_tag=1, requeue=False), 1), from_broker=False)
    assert records[0]["payload"] == {}
    assert records[0]["ack_count"] == 0
    assert records[0]["settlement"] == "reject"
