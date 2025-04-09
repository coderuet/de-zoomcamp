DC = docker compose

.PHONY: kafka-build kafka-down submit-job
kafka-build:
	@echo "Start building kafka"
	@cd $(DC) -f stream-compose.yaml up -d --force-recreate

kafka-down:
	@echo "Stopping kafka"
	@$(DC) down

submit-job:
	@echo "Submit job"
	@flink run $(filter-out $@,$(MAKECMDGOALS))