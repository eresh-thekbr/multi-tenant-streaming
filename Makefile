PYTHON ?= python3
KAFKA_TOPICS = docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092
STRATEGY ?= tenant-fair
RUN_SECONDS ?= 60

.PHONY: install test demo kafka-up kafka-down produce consume

install:
	$(PYTHON) -m pip install -r requirements-dev.txt

test:
	$(PYTHON) -m pytest -q

demo:
	$(PYTHON) -m tenant_fair_streaming demo

kafka-up:
	docker compose up -d kafka
	@echo "waiting for Kafka to accept connections..."
	@until $(KAFKA_TOPICS) --list >/dev/null 2>&1; do sleep 1; done
	$(KAFKA_TOPICS) --create --if-not-exists --topic collections.conversation.events --partitions 6 --replication-factor 1
	$(KAFKA_TOPICS) --create --if-not-exists --topic collections.conversation.events.dlq --partitions 1 --replication-factor 1

kafka-down:
	docker compose down -v

produce:
	$(PYTHON) -m tenant_fair_streaming produce --rate 200 --seconds $(RUN_SECONDS)

consume:
	$(PYTHON) -m tenant_fair_streaming consume --strategy $(STRATEGY) --seconds $(RUN_SECONDS)
