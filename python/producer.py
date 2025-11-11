import argparse, time, random, uuid
from datetime import datetime
from kafka import KafkaProducer
from json import dumps

def make_producer(bootstrap):
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda x: dumps(x).encode('utf-8')
    )

def generate_message(sensor_id):
    now = datetime.now()
    car = random.randint(0,4)
    bus = random.randint(0,2)
    truck = random.randint(0,2)
    jeepney = random.randint(0, 2)
    bike = random.randint(0, 5)
    tryke = random.randint(0, 3)
    others = random.randint(0, 2)
    total = car + bus + truck + jeepney + bike + tryke + others

    return {
        "uuid_id": str(uuid.uuid4()),
        "lgu_code": "1200",
        "sensor_id": sensor_id,
        "date_saved": now.strftime('%m/%d/%Y'),
        "time_saved": now.strftime("%X"),
        "total": total,
        "car": car,
        "bus": bus,
        "truck": truck,
        "jeepney": jeepney,
        "bike": bike,
        "tryke": tryke,
        "others": others
    }

def main(args):
    p = make_producer(args.bootstrap.split(","))
    print(f"Producer connected to {args.bootstrap}, topic {args.topic} as {args.sensor}")
    while True:
        msg = generate_message(args.sensor)
        p.send(args.topic, msg)
        p.flush()
        print(f"[{args.sensor}] sent {msg['uuid_id']} total={msg['total']}")
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sensor", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap", default="kafka:9092")
    args = parser.parse_args()
    main(args)