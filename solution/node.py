import hashlib
from anysystem import Context, Message, Process
from typing import List, Optional, Dict, Tuple

class StorageNode(Process):
    def __init__(self, node_id: str, nodes: List[str]):
        self._id = node_id
        self._nodes = sorted(nodes)
        self._node_count = len(self._nodes)
        self._data = {}
        self._pending_requests = {}
        self._request_counter = 0

    def on_local_message(self, msg: Message, ctx: Context):
        if msg.type == 'GET':
            self._handle_local_get(msg, ctx)
        elif msg.type == 'PUT':
            self._handle_local_put(msg, ctx)
        elif msg.type == 'DELETE':
            self._handle_local_delete(msg, ctx)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg.type == 'REPLICA_GET_REQ':
            self._handle_replica_get_req(msg, sender, ctx)
        elif msg.type == 'REPLICA_GET_RESP':
            self._handle_replica_get_resp(msg, sender, ctx)
        elif msg.type == 'REPLICA_PUT_REQ':
            self._handle_replica_put_req(msg, sender, ctx)
        elif msg.type == 'REPLICA_PUT_RESP':
            self._handle_replica_put_resp(msg, sender, ctx)
        elif msg.type == 'REPLICA_DELETE_REQ':
            self._handle_replica_delete_req(msg, sender, ctx)
        elif msg.type == 'REPLICA_DELETE_RESP':
            self._handle_replica_delete_resp(msg, sender, ctx)
        elif msg.type == 'REPLICA_READ_REPAIR':
            self._handle_replica_read_repair(msg, sender, ctx)

    def on_timer(self, timer_name: str, ctx: Context):
        pass

    def _handle_local_get(self, msg: Message, ctx: Context):
        key = msg['key']
        quorum = msg['quorum']
        replicas = get_key_replicas(key, self._node_count)
        request_id = self._request_counter
        self._request_counter += 1
        self._pending_requests[request_id] = {
            'operation': 'GET',
            'key': key,
            'replicas': replicas,
            'quorum': quorum,
            'responses': {},
            'timestamp_sent': ctx.time(),
        }
        for replica in replicas:
            internal_msg = Message('REPLICA_GET_REQ', {
                'key': key,
                'request_id': request_id,
                'coordinator': self._id,
            })
            ctx.send(internal_msg, replica)

    def _handle_local_put(self, msg: Message, ctx: Context):
        key = msg['key']
        value = msg['value']
        quorum = msg['quorum']
        replicas = get_key_replicas(key, self._node_count)
        request_id = self._request_counter
        self._request_counter += 1
        timestamp = ctx.time()
        self._pending_requests[request_id] = {
            'operation': 'PUT',
            'key': key,
            'value': value,
            'replicas': replicas,
            'quorum': quorum,
            'responses': {},
            'timestamp_sent': timestamp,
            'timestamp': timestamp,
        }
        for replica in replicas:
            internal_msg = Message('REPLICA_PUT_REQ', {
                'key': key,
                'value': value,
                'request_id': request_id,
                'coordinator': self._id,
                'timestamp': timestamp,
            })
            ctx.send(internal_msg, replica)

    def _handle_local_delete(self, msg: Message, ctx: Context):
        key = msg['key']
        quorum = msg['quorum']
        replicas = get_key_replicas(key, self._node_count)
        request_id = self._request_counter
        self._request_counter += 1
        timestamp = ctx.time()
        self._pending_requests[request_id] = {
            'operation': 'DELETE',
            'key': key,
            'replicas': replicas,
            'quorum': quorum,
            'responses': {},
            'timestamp_sent': timestamp,
            'timestamp': timestamp,
        }
        for replica in replicas:
            internal_msg = Message('REPLICA_DELETE_REQ', {
                'key': key,
                'request_id': request_id,
                'coordinator': self._id,
                'timestamp': timestamp,
            })
            ctx.send(internal_msg, replica)

    def _handle_replica_get_req(self, msg: Message, sender: str, ctx: Context):
        key = msg['key']
        request_id = msg['request_id']
        coordinator = msg['coordinator']
        value, timestamp = self._data.get(key, (None, -1.0))
        resp_msg = Message('REPLICA_GET_RESP', {
            'key': key,
            'value': value,
            'timestamp': timestamp,
            'request_id': request_id,
            'replica': self._id,
        })
        ctx.send(resp_msg, coordinator)

    def _handle_replica_get_resp(self, msg: Message, sender: str, ctx: Context):
        request_id = msg['request_id']
        replica = msg['replica']
        if request_id not in self._pending_requests:
            return
        pending = self._pending_requests[request_id]
        value = msg['value']
        timestamp = msg['timestamp']
        pending['responses'][replica] = (value, timestamp)
        if len(pending['responses']) >= pending['quorum']:
            self._finalize_get(request_id, ctx)

    def _finalize_get(self, request_id: int, ctx: Context):
        pending = self._pending_requests.pop(request_id)
        key = pending['key']
        replicas = pending['replicas']
        best_value = None
        best_timestamp = -1.0
        for replica, (value, timestamp) in pending['responses'].items():
            if timestamp > best_timestamp:
                best_timestamp = timestamp
                best_value = value
            elif timestamp == best_timestamp and value is not None and best_value is not None:
                if value > best_value:
                    best_value = value
        for replica, (value, timestamp) in pending['responses'].items():
            if timestamp < best_timestamp or (timestamp == best_timestamp and value != best_value):
                repair_msg = Message('REPLICA_READ_REPAIR', {
                    'key': key,
                    'value': best_value,
                    'timestamp': best_timestamp,
                })
                ctx.send(repair_msg, replica)
        resp_msg = Message('GET_RESP', {
            'key': key,
            'value': best_value,
        })
        ctx.send_local(resp_msg)

    def _handle_replica_read_repair(self, msg: Message, sender: str, ctx: Context):
        key = msg['key']
        value = msg['value']
        timestamp = msg['timestamp']
        current_value, current_timestamp = self._data.get(key, (None, -1.0))
        should_update = False
        if timestamp > current_timestamp:
            should_update = True
        elif timestamp == current_timestamp:
            if value is not None and current_value is not None:
                should_update = value > current_value
            elif value is not None:
                should_update = True
        if should_update:
            self._data[key] = (value, timestamp)

    def _handle_replica_put_req(self, msg: Message, sender: str, ctx: Context):
        key = msg['key']
        value = msg['value']
        request_id = msg['request_id']
        coordinator = msg['coordinator']
        timestamp = msg['timestamp']
        current_value, current_timestamp = self._data.get(key, (None, -1.0))
        should_update = False
        if timestamp > current_timestamp:
            should_update = True
        elif timestamp == current_timestamp:
            if value is not None and current_value is not None:
                should_update = value > current_value
            elif value is not None:
                should_update = True
        if should_update:
            self._data[key] = (value, timestamp)
        else:
            value = current_value
            timestamp = current_timestamp
        resp_msg = Message('REPLICA_PUT_RESP', {
            'key': key,
            'value': value,
            'timestamp': timestamp,
            'request_id': request_id,
            'replica': self._id,
        })
        ctx.send(resp_msg, coordinator)

    def _handle_replica_put_resp(self, msg: Message, sender: str, ctx: Context):
        request_id = msg['request_id']
        replica = msg['replica']
        if request_id not in self._pending_requests:
            return
        pending = self._pending_requests[request_id]
        if pending['operation'] != 'PUT':
            return
        value = msg['value']
        timestamp = msg['timestamp']
        pending['responses'][replica] = (value, timestamp)
        if len(pending['responses']) >= pending['quorum']:
            self._finalize_put(request_id, ctx)

    def _finalize_put(self, request_id: int, ctx: Context):
        pending = self._pending_requests.pop(request_id)
        key = pending['key']
        best_value = None
        best_timestamp = -1.0
        for replica, (value, timestamp) in pending['responses'].items():
            if timestamp > best_timestamp:
                best_timestamp = timestamp
                best_value = value
            elif timestamp == best_timestamp and value is not None and best_value is not None:
                if value > best_value:
                    best_value = value
        resp_msg = Message('PUT_RESP', {
            'key': key,
            'value': best_value,
        })
        ctx.send_local(resp_msg)

    def _handle_replica_delete_req(self, msg: Message, sender: str, ctx: Context):
        key = msg['key']
        request_id = msg['request_id']
        coordinator = msg['coordinator']
        timestamp = msg['timestamp']
        current_value, current_timestamp = self._data.get(key, (None, -1.0))
        self._data[key] = (None, timestamp)
        resp_msg = Message('REPLICA_DELETE_RESP', {
            'key': key,
            'value': current_value,
            'timestamp': current_timestamp,
            'request_id': request_id,
            'replica': self._id,
        })
        ctx.send(resp_msg, coordinator)

    def _handle_replica_delete_resp(self, msg: Message, sender: str, ctx: Context):
        request_id = msg['request_id']
        replica = msg['replica']
        if request_id not in self._pending_requests:
            return
        pending = self._pending_requests[request_id]
        if pending['operation'] != 'DELETE':
            return
        value = msg['value']
        timestamp = msg['timestamp']
        pending['responses'][replica] = (value, timestamp)
        if len(pending['responses']) >= pending['quorum']:
            self._finalize_delete(request_id, ctx)

    def _finalize_delete(self, request_id: int, ctx: Context):
        pending = self._pending_requests.pop(request_id)
        key = pending['key']
        best_value = None
        best_timestamp = -1.0
        for replica, (value, timestamp) in pending['responses'].items():
            if timestamp > best_timestamp:
                best_timestamp = timestamp
                best_value = value
            elif timestamp == best_timestamp and value is not None and best_value is not None:
                if value > best_value:
                    best_value = value
        resp_msg = Message('DELETE_RESP', {
            'key': key,
            'value': best_value,
        })
        ctx.send_local(resp_msg)

def get_key_replicas(key: str, node_count: int):
    replicas = []
    key_hash = int.from_bytes(hashlib.md5(key.encode('utf8')).digest(), 'little', signed=False)
    cur = key_hash % node_count
    for _ in range(3):
        replicas.append(str(cur))
        cur = get_next_replica(cur, node_count)
    return replicas

def get_next_replica(i, node_count: int):
    return (i + 1) % node_count