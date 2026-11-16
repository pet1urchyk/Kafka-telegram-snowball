from confluent_kafka import TopicPartition,KafkaException
BOOTSTRAP = "localhost:19092,localhost:29092,localhost:39092"

FOUND = "tg.found"
SEEDS = "tg.seeds"
REGISTRY = "tg.registry"
EDGES = "tg.edges"
DLQ = "tg.dlq"

MAX_DEPTH = 1
BATCH_SIZE = 10
WORKER_BATCH = 1

def commit_positions(msgs):
    pos = {}
    for m in msgs:
        if m.error():
            continue
        key = (m.topic(), m.partition())
        pos[key] = max(pos.get(key, -1), m.offset() + 1)
    return [TopicPartition(t, p, o) for (t, p), o in pos.items()]


def rewind(consumer, msgs):
    first = {}
    for m in msgs:
        if m.error():
            continue
        key = (m.topic(), m.partition())
        first[key] = min(first.get(key, m.offset()), m.offset())
    for (t, p), o in first.items():
        consumer.seek(TopicPartition(t, p, o))


def commit_with_retry(producer, tries=3):
    for _ in range(tries):
        try:
            producer.commit_transaction()
            return
        except KafkaException as e:
            if e.args[0].retriable():
                continue
            raise
    raise RuntimeError("commit: вичерпано спроби")


def dlq_payload(msg, error):
    return {
        "error": str(error),
        "source": {"topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset()},
        "raw": msg.value().decode("utf-8", errors="replace") if msg.value() else None,
    }
def normalize_channel_id(raw_id):
    if raw_id is None:
        return None
    s = str(raw_id).strip()
    s = s.replace("https://t.me/", "").replace("http://t.me/", "").lstrip("@")
    if s.isdigit():
        return int(s)
    return s.lower()