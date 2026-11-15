from common import BOOTSTRAP, FOUND, EDGES, DLQ, WORKER_BATCH, commit_positions, dlq_payload, commit_with_retry, rewind, SEEDS
import json
import sys
import asyncio
import logging
import re
from confluent_kafka import Producer, Consumer, KafkaException
from config import Config
from utils import ensure_telegram_credentials, attempt_connection_to_telegram
from recommendations import get_channel_recommendations

logging.basicConfig(level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

worker_id = sys.argv[1] if len(sys.argv) > 1 else "1"

c = Consumer({
    'bootstrap.servers': BOOTSTRAP,
    'group.id': 'tg.workers',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,       
    'isolation.level': 'read_committed',
    'max.poll.interval.ms': 600000,
})
p = Producer({
    'bootstrap.servers': BOOTSTRAP,
    'transactional.id': f'tg.worker-{worker_id}', 
    'enable.idempotence': True          
})

def delivery(err, msg):
    if err is not None:
        logger.error(f"FAIL: {err}")

def extract_usernames(text):
    if not text:
        return []
    return re.findall(r"(?:https?://)?t\.me/([a-zA-Z0-9_]{5,})", text)

async def process_seed(client, event):
    channel_identifier = event["channel_id"]
    run_id = event["run_id"]
    current_depth = event["depth"]
    
    found = []
    edges = []
    
    entity = await client.get_entity(channel_identifier)
    channel_id = entity.id
    channel_name = getattr(entity, "title", "Unknown")
    channel_username = getattr(entity, "username", "Unknown")

    if Config.DEFAULT_INCLUDE_RECOMMENDATIONS:
        try:
            rec_channels = await get_channel_recommendations(
                client, entity, max_depth=Config.DEFAULT_RECOMMENDATIONS_DEPTH,
                edge_list_writer=None, link_records=[], run_name=run_id, entity_records=[], request_timeout=15.0
            )
            for rec_id in rec_channels:
                found.append({
                    "run_id": run_id, "channel_id": rec_id, "depth": current_depth + 1,
                    "discovered_from": channel_id, "found_via": "recommendation"
                })
                edges.append({
                    "run_id": run_id, "from_id": channel_id, "to_id": rec_id, "type": "recommendation",
                    "evidence": "telegram_recommendation", "from_name": channel_name, "from_username": channel_username,
                    "to_name": "Recommended", "to_username": ""
                })
        except Exception:
            pass

    async for message in client.iter_messages(entity, limit=Config.DEFAULT_MAX_POSTS or 50):
        if message.forward and message.forward.chat:
            fwd_chat = message.forward.chat
            fwd_id = getattr(fwd_chat, "id", None)
            if fwd_id:
                fwd_name = getattr(fwd_chat, "title", "Unknown")
                fwd_username = getattr(fwd_chat, "username", "Unknown")
                found.append({
                    "run_id": run_id, "channel_id": fwd_id, "depth": current_depth + 1,"title": fwd_name,"username": fwd_username,
                    "discovered_from": channel_id, "found_via": "forward"
                })
                edges.append({
                    "run_id": run_id, "from_id": fwd_id, "to_id": channel_id, "type": "forward",
                    "evidence": f"msg_{message.id}", "from_name": fwd_name, "from_username": fwd_username,
                    "to_name": channel_name, "to_username": channel_username
                })

        if message.message:
            for uname in extract_usernames(message.message):
                edges.append({
                    "run_id": run_id, "from_id": channel_id, "to_id": uname, "type": "outbound_link",
                    "evidence": f"text_link_msg_{message.id}", "from_name": channel_name, "from_username": channel_username,
                    "to_name": uname, "to_username": uname
                })
                found.append({
                    "run_id": run_id, "channel_id": uname, "depth": current_depth + 1,
                    "discovered_from": channel_id, "found_via": "outbound_link"
                })

    return found, edges

async def main_async():
    api_id, api_hash = ensure_telegram_credentials()
    client = await attempt_connection_to_telegram(api_id, api_hash)

    c.subscribe([SEEDS])
    p.init_transactions()
    
    try:
        while True:
            msgs = c.consume(num_messages=WORKER_BATCH, timeout=1.0)
            if not msgs:
                await asyncio.sleep(0.1)
                continue
            positions = commit_positions(msgs)
            if not positions:
                continue

            out_found, out_edges, out_dlq = [], [], []
            
            for msg in msgs:
                if msg.error():
                    if msg.error().fatal():
                        raise KafkaException(msg.error())
                    continue
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    found, edges = await process_seed(client, event)
                    out_found.extend(found)
                    out_edges.extend(edges)
                except Exception as e:
                    out_dlq.append(dlq_payload(msg, e))

            try:
                p.begin_transaction()
                for f in out_found:
                    p.produce(FOUND, key=str(f["channel_id"]), value=json.dumps(f), callback=delivery)
                for e in out_edges:
                    key = f"{e['from_id']}|{e['to_id']}|{e['type']}|{e['evidence']}"
                    p.produce(EDGES, key=key, value=json.dumps(e), callback=delivery)
                for d in out_dlq:
                    p.produce(DLQ, value=json.dumps(d), callback=delivery)
                
                p.poll(0)
                p.send_offsets_to_transaction(positions, c.consumer_group_metadata(), 10.0)
                commit_with_retry(p)
            except Exception as e:
                try:
                    p.abort_transaction()
                except Exception:
                    pass
                rewind(c, msgs)
    finally:
        await client.disconnect()
        c.close()

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()