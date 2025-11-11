import argparse, json
from kafka import KafkaConsumer

def main(args):
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print(f"Consumer listening to {args.topic} on {args.bootstrap}")
    for msg in consumer:
        print("MSG:", msg.value)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap", default="kafka:9092")
    args = parser.parse_args()
    main(args)