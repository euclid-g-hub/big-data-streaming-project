#!/bin/sh
set -e

SERVICE="${SERVICE:-producer}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC="${KAFKA_TOPIC:-cctv_vehicle_counts}"

export KAFKA_BOOTSTRAP_SERVERS="$KAFKA_BOOTSTRAP"
export KAFKA_TOPIC="$TOPIC"

derive_sensor_id() {
  if [ -n "$SENSOR_ID" ]; then
    echo "$SENSOR_ID"
  else
    
    idx=$(echo "$HOSTNAME" | awk -F'_' '{print $NF}')
    
    if echo "$idx" | grep -q '^[0-9]\+$'; then
      printf "sensor_%02d" "$idx"
    else
      echo "sensor_01"
    fi
  fi
}

case "$SERVICE" in
  producer)
    SENSOR="$(derive_sensor_id)"
    echo "Starting producer with SENSOR_ID=$SENSOR"
    exec python producer.py --sensor "$SENSOR" --topic "$KAFKA_TOPIC" --bootstrap "$KAFKA_BOOTSTRAP_SERVERS"
    ;;
  consumer)
    echo "Starting consumer..."
    exec python consumer.py --topic "$KAFKA_TOPIC" --bootstrap "$KAFKA_BOOTSTRAP_SERVERS"
    ;;
  kafka_to_cassandra)
    echo "Starting kafka->cassandra sink..."
    exec python kafka_to_cassandra.py --topic "$KAFKA_TOPIC" --bootstrap "$KAFKA_BOOTSTRAP_SERVERS" --cassandra "$CASSANDRA_CONTACT_POINTS" --keyspace "$CASSANDRA_KEYSPACE"
    ;;
  *)
    echo "Unknown SERVICE: $SERVICE"
    exit 2
    ;;
esac