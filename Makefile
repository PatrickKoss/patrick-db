run-all-docker: run-zookeeper
	docker-compose up router server server2 server3 server4 envoy

run-zookeeper:
	docker-compose up zoo1 zoo2 zoo3

test:
	for dir in $$(find . -type d); do \
		if [ -f $$dir/Cargo.toml ]; then \
			echo "Running tests in $$dir"; \
			(cd $$dir && cargo test); \
		fi \
	done

lint:
	for dir in $$(find . -type d); do \
		if [ -f $$dir/Cargo.toml ]; then \
			echo "Running lint in $$dir"; \
			(cd $$dir && cargo clippy); \
		fi \
	done
