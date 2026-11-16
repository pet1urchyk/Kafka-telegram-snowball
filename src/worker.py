from common import BOOTSTRAP, FOUND, EDGES, DLQ, WORKER_BATCH, commit_positions, dlq_payload, commit_with_retry, rewind, SEEDS,normalize_channel_id
import json
import sys
import asyncio
import logging
import re
from confluent_kafka import Producer, Consumer, KafkaException
from config import Config
from utils import ensure_telegram_credentials, attempt_connection_to_telegram
from recommendations import get_channel_recommendations
from telethon.tl.types import Channel
from telethon.errors import FloodWaitError

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

def extract_usernames(text):
    if not text:
        return []
    matches = re.findall(r"(?:https?://)?t\.me/([a-zA-Z0-9_]{5,})", text)
    ignored = {"joinchat", "addstickers", "share", "proxy", "socks", "iv"}
    return [m for m in matches if m.lower() not in ignored]

def delivery(err, msg):
    if err is not None:
        logger.error(f"FAIL: {err}")

async def process_seed(client, event):
    channel_identifier = normalize_channel_id(event["channel_id"])
    run_id = event["run_id"]
    current_depth = event["depth"]
    
    found = []
    edges = []
    
    entity = await client.get_entity(channel_identifier)
    if not (isinstance(entity, Channel) and getattr(entity, "broadcast", False)):
        raise ValueError(f"not_a_channel: {channel_identifier}")
    channel_id = entity.id
    channel_name = getattr(entity, "title", "Unknown")
    channel_username = getattr(entity, "username", "Unknown")

    if Config.DEFAULT_INCLUDE_RECOMMENDATIONS:
        try:
            rec_entities = []
            await get_channel_recommendations(
                client, entity, max_depth=Config.DEFAULT_RECOMMENDATIONS_DEPTH,
                edge_list_writer=None, link_records=[], run_name=run_id,
                entity_records=rec_entities, request_timeout=15.0
            )
            for rec in rec_entities:
                raw_rec_id = rec.get("entity_id") or rec.get("username")
                norm_rec_id = normalize_channel_id(raw_rec_id)
                if not norm_rec_id:
                    continue

                rec_name = rec.get("name") or "Unknown"
                rec_username = rec.get("username") or ""

                found.append({
                    "run_id": run_id,
                    "channel_id": norm_rec_id,
                    "title": rec_name,
                    "username": rec_username,
                    "depth": current_depth + 1,
                    "discovered_from": normalize_channel_id(channel_id),
                    "found_via": "recommendation"
                })
                edges.append({
                    "run_id": run_id,
                    "from_id": normalize_channel_id(channel_id),
                    "to_id": norm_rec_id,
                    "type": "recommendation",
                    "evidence": "telegram_recommendation",
                    "from_name": channel_name,
                    "from_username": channel_username,
                    "to_name": rec_name,
                    "to_username": rec_username
                })
        except Exception as e:
            logger.warning(f"Recs failed for {channel_id}: {type(e).__name__}: {e}")

    async for message in client.iter_messages(entity, limit=Config.DEFAULT_MAX_POSTS or 50):
        if message.forward and message.forward.chat:
            fwd_chat = message.forward.chat
            fwd_id = getattr(fwd_chat, "id", None)
            if fwd_id:
                fwd_name = getattr(fwd_chat, "title", "Unknown")
                fwd_username = getattr(fwd_chat, "username", "Unknown")
                norm_fwd_id = normalize_channel_id(fwd_id)
                norm_ch_id = normalize_channel_id(channel_id)
                found.append({
                    "run_id": run_id,
                    "channel_id": norm_fwd_id,
                    "depth": current_depth + 1,
                    "title": fwd_name,
                    "username": fwd_username,
                    "discovered_from": norm_ch_id,
                    "found_via": "forward"
                })
                edges.append({
                    "run_id": run_id,
                    "from_id": norm_fwd_id,
                    "to_id": norm_ch_id,
                    "type": "forward",
                    "evidence": f"msg_{message.id}",
                    "from_name": fwd_name,
                    "from_username": fwd_username,
                    "to_name": channel_name,
                    "to_username": channel_username
                })  

        if message.message:
            for uname in extract_usernames(message.message):
                norm_uname = normalize_channel_id(uname)
                norm_ch_id = normalize_channel_id(channel_id)
                edges.append({
                    "run_id": run_id,
                    "from_id": norm_ch_id,
                    "to_id": norm_uname,
                    "type": "outbound_link",
                    "evidence": f"text_link_msg_{message.id}",
                    "from_name": channel_name,
                    "from_username": channel_username,
                    "to_name": norm_uname,
                    "to_username": norm_uname
                })
                found.append({
                    "run_id": run_id,
                    "channel_id": norm_uname,
                    "title":uname,
                    "username":uname,
                    "depth": current_depth + 1,
                    "discovered_from": norm_ch_id,
                    "found_via": "outbound_link"
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
            temp_error_occurred = False
            for msg in msgs:
                if msg.error():
                    if msg.error().fatal():
                        raise KafkaException(msg.error())
                    continue
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                    found, edges = await process_seed(client, event)
                    for f in found:
                        out_found.append((str(f["channel_id"]), json.dumps(f)))
                    for e in edges:
                        key = f"{e['from_id']}|{e['to_id']}|{e['type']}|{e['evidence']}"
                        out_edges.append((key, json.dumps(e)))
                except (FloodWaitError, ConnectionError, TimeoutError) as temp_e:
                    logger.warning(f"Тимчасова помилка [{type(temp_e).__name__}]: {temp_e}. Відкат батча.")
                    temp_error_occurred = True
                    break
                except Exception as perm_e:
                    logger.error(f"Постійна помилка [{type(perm_e).__name__}]: {perm_e}")
                    out_dlq.append(json.dumps(dlq_payload(msg, perm_e)))
            if temp_error_occurred:
                await asyncio.sleep(10)
                rewind(c, msgs)
                continue
            try:
                p.begin_transaction()
                for k,val in out_found:
                    p.produce(FOUND, key=k, value=val, callback=delivery)
                for k,val in out_edges:
                    p.produce(EDGES, key=k, value=val, callback=delivery)
                for val in out_dlq:
                    p.produce(DLQ, value=val, callback=delivery)
                
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