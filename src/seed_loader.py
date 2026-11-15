from common import BOOTSTRAP, FOUND
import json
import sys
from confluent_kafka import Producer

def delivery(err,msg):
    if err is not None:
        print(f"Error.{err}")
    else:
        print(f"message delivered succesfully, {msg.value()}")

def main():
    if len(sys.argv) < 3:
        print("Usage: python seed_loader.py <channel_id_or_username> <run_id>")
        sys.exit(1)

    channel_input = sys.argv[1]
    run_id = sys.argv[2]
    producer_conf = {
        'bootstrap.servers': BOOTSTRAP,
        'acks': 'all',
        'enable.idempotence': True          
    }
    p = Producer(producer_conf)
    event = {
        "run_id": run_id,
        "channel_id": channel_input, 
        "depth": 0,
        "discovered_from": None,
        "found_via": "seed"
    }
    p.produce(
        topic=FOUND,
        key=str(channel_input),
        value=json.dumps(event),
        callback=delivery
    )
    p.flush()
    print(f"Seed channel {channel_input} injected into {FOUND} for run '{run_id}'.")

if __name__ == "__main__":
    main()
