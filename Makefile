.PHONY: install-schemas
install-schemas:
	cd ksml-examples/ksml-demo-producer && mvn clean package

.PHONY: local-server
local-server: install-schemas
	docker-compose up
	docker-compose down

