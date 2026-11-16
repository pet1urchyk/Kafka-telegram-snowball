from common import BOOTSTRAP,FOUND,SEEDS,DLQ,REGISTRY,BATCH_SIZE,MAX_DEPTH,commit_with_retry,commit_positions,dlq_payload,rewind,normalize_channel_id
import json
from confluent_kafka import Producer,Consumer,TopicPartition,KafkaError,KafkaException, OFFSET_BEGINNING
import sys
import time
router_id = sys.argv[1]
conf_c = {
    "bootstrap.servers":BOOTSTRAP,
    "group.id":"tg.router",
    "auto.offset.reset":"earliest",
    "enable.auto.commit":False,  
    "isolation.level": "read_committed",
    'max.poll.interval.ms': 600000,
}
conf_p = {
    "bootstrap.servers":BOOTSTRAP,
    "enable.idempotence":True,
    "acks":"all",
    "transactional.id":f"router-1-{router_id}"
}
c = Consumer(conf_c)
p = Producer(conf_p)
p.init_transactions()


def delivery(err,msg):
    if err is not None:
        print(f"Error.{err}")
    else:
        print(f"message delivered succesfully, {msg.value()}")

def decide(event, seen, batch_new, max_depth):
    channel_id = normalize_channel_id(event["channel_id"])
    channel_title = normalize_channel_id(event["title"])
    depth = event["depth"]
    if channel_id in seen or channel_id in batch_new:
        return "duplicate"
    if channel_title in seen or channel_title in batch_new:
        return "duplicate"
    if depth > max_depth:
        return "too_deep"
    return "new"
    
def restore_seen():
    restore_c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "tg-router-restore",
        "enable.auto.commit": False,
        "enable.partition.eof": True,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    md = restore_c.list_topics(REGISTRY, timeout=10)
    partitions = list(md.topics[REGISTRY].partitions)
    restore_c.assign([TopicPartition(REGISTRY, p, OFFSET_BEGINNING) for p in partitions])
    seen = {}
    done = set()
    while len(done) < len(partitions):
        msg = restore_c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                done.add(msg.partition())
                continue
            raise KafkaException(msg.error())
        raw_key = msg.key().decode()
        key = normalize_channel_id(raw_key)
        value = msg.value()
        if value is None:
            seen.pop(key, None)
        else:
            seen[key] = json.loads(value)["depth"]
    restore_c.close()
    return seen
def main():
    seen = restore_seen()
    c.subscribe([FOUND])
    try:
        while True:
            msgs = c.consume(num_messages=BATCH_SIZE, timeout=1.0)   
            if not msgs:
                continue
            positions = commit_positions(msgs)
            if not positions:   
                continue
            batch_new = {}
            stats = {"new": 0, "duplicate": 0, "too_deep": 0, "budget": 0, "dlq": 0}
            try:
                p.begin_transaction()
                for msg in msgs:
                    if msg.error():
                        if msg.error().fatal():
                            raise KafkaException(msg.error())
                        print(f"consumer error: {msg.error()}")
                        continue
                    event = None
                    try:
                        event = json.loads(msg.value().decode("utf-8"))
                        channel_id = normalize_channel_id(event["channel_id"])
                        result = decide(event, seen, batch_new, MAX_DEPTH)
                    except Exception as e:
                        p.produce(DLQ, key=msg.key(), value=json.dumps(dlq_payload(msg, e)),
                                  callback=delivery)
                        stats["dlq"] += 1
                        continue

                    stats[result] += 1
                    if result == "new":
                        registry_event = {"status": "queued", "depth": event["depth"],
                                          "run_id": event["run_id"]}
                        p.produce(REGISTRY, key=str(channel_id), value=json.dumps(registry_event),
                                  callback=delivery)
                        p.produce(SEEDS, key=str(channel_id), value=json.dumps(event),
                                  callback=delivery)
                        batch_new[channel_id] = event["depth"]
                    p.poll(0)

                p.send_offsets_to_transaction(positions, c.consumer_group_metadata(), 10.0)
                commit_with_retry(p)
                seen.update(batch_new)   
                print(f"batch {len(msgs)}: {stats} | and {len(seen)}")
            except Exception as e:
                time.sleep(2)
                if isinstance(e, KafkaException) and e.args[0].fatal():
                    raise
                try:
                    p.abort_transaction()
                except Exception:
                    pass
                rewind(c, msgs)

    except KeyboardInterrupt:
        print("Stopping..")
        try:
            p.abort_transaction()
        except Exception:
            pass
    finally:
        c.close()

if __name__ == "__main__":
    main()

