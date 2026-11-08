"""Observe test AMQP deliveries/ACKs; callers relay the original bytes unchanged."""

import json
from uuid import uuid4

from pamqp import body, frame, header
from pamqp.commands import Basic


class AMQPObserver:
    def __init__(self, records: list) -> None:
        self.records = records
        self.connection_id = uuid4().hex
        self._buffers = {True: bytearray(), False: bytearray()}
        self._pending = {}
        self._unsettled = {}

    def feed(self, data: bytes, *, from_broker: bool) -> None:
        buffer = self._buffers[from_broker]
        buffer.extend(data)
        while len(buffer) >= 8:
            size = 8 if buffer[:4] == b"AMQP" else 8 + int.from_bytes(buffer[3:7], "big")
            if size > 2 * 1024 * 1024:
                raise ValueError("Oversized frame in isolated test proxy")
            if len(buffer) < size:
                return
            consumed, channel, value = frame.unmarshal(bytes(buffer[:size]))
            del buffer[:consumed]
            if from_broker:
                self._received(channel, value)
            elif isinstance(value, (Basic.Ack, Basic.Reject, Basic.Nack)):
                self._settled(channel, value)

    def _received(self, channel, value) -> None:
        if isinstance(value, Basic.Deliver):
            self._pending[channel] = {
                "record": {
                    "connection_id": self.connection_id, "channel": channel,
                    "delivery_tag": value.delivery_tag, "redelivered": value.redelivered,
                    "ack_count": 0, "settlement": None,
                },
                "body": bytearray(), "size": None,
            }
        elif channel in self._pending:
            pending = self._pending[channel]
            if isinstance(value, header.ContentHeader):
                pending["size"] = value.body_size
            elif isinstance(value, body.ContentBody):
                pending["body"].extend(value.value)
            if pending["size"] == len(pending["body"]):
                try:
                    payload = json.loads(pending["body"])
                except (ValueError, UnicodeError):
                    payload = {}
                record = pending["record"]
                record["payload"] = payload if isinstance(payload, dict) else {}
                self.records.append(record)
                self._unsettled[channel, record["delivery_tag"]] = record
                del self._pending[channel]

    def _settled(self, channel, value) -> None:
        multiple = getattr(value, "multiple", False)
        for key, record in list(self._unsettled.items()):
            same_tag = key[1] == value.delivery_tag
            in_range = multiple and (value.delivery_tag == 0 or key[1] <= value.delivery_tag)
            if key[0] == channel and (same_tag or in_range):
                if isinstance(value, Basic.Ack):
                    record["ack_count"] += 1
                    record["settlement"] = "ack"
                else:
                    record["settlement"] = "reject" if isinstance(value, Basic.Reject) else "nack"
                del self._unsettled[key]
