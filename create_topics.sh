docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker-1:9092 --create --if-not-exists \
  --topic tg.found --partitions 6 --replication-factor 3 --config retention.ms=604800000
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker-1:9092 --create --if-not-exists \
  --topic tg.seeds --partitions 6 --replication-factor 3 --config retention.ms=604800000
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker-1:9092 --create --if-not-exists \
  --topic tg.registry --partitions 6 --replication-factor 3 --config cleanup.policy=compact
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker-1:9092 --create --if-not-exists \
  --topic tg.edges --partitions 3 --replication-factor 3 --config cleanup.policy=compact
docker compose exec broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker-1:9092 --create --if-not-exists \
  --topic tg.dlq --partitions 1 --replication-factor 3 --config retention.ms=604800000 

