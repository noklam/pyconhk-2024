cleanup:
	duckdb tpch.duckdb < scripts/clean_up.sh


build-docker:
	docker build -t .Dockerfile .

run-docker:
	docker run -it -v "$(pwd)":/opt/spark/app pycon /bin/bash