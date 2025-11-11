import argparse, json
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

def ensure_schema(session, keyspace, table):
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
    """)
    session.set_keyspace(keyspace)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            uuid_id text PRIMARY KEY,
            lgu_code text,
            sensor_id text,
            date_saved text,
            time_saved text,
            total int,
            car int,
            bus int,
            truck int,
            jeepney int,
            bike int,
            tryke int,
            others int
        );
    """)

def main(args):
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=[args.bootstrap],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    cluster = Cluster([args.cassandra])
    session = cluster.connect()
    ensure_schema(session, args.keyspace, "vehicle_counts")
    insert = session.prepare("""
        INSERT INTO vehicle_counts (uuid_id, lgu_code, sensor_id, date_saved, time_saved, total, car, bus, truck, jeepney, bike, tryke, others)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    print("Kafka->Cassandra sink started...")
    for m in consumer:
        d = m.value
        try:
            session.execute(insert, (
                d.get('uuid_id'),
                d.get('lgu_code'),
                d.get('sensor_id'),
                d.get('date_saved'),
                d.get('time_saved'),
                int(d.get('total', 0)),
                int(d.get('car', 0)),
                int(d.get('bus', 0)),
                int(d.get('truck', 0)),
                int(d.get('jeepney', 0)),
                int(d.get('bike', 0)),
                int(d.get('tryke', 0)),
                int(d.get('others', 0))
            ))
            print("Inserted:", d.get('uuid_id'))
        except Exception as e:
            print("Insert error:", e)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap", default="kafka:9092")
    parser.add_argument("--cassandra", default="cassandra")
    parser.add_argument("--keyspace", default="trafficks")
    args = parser.parse_args()
    main(args)