.DEFAULT_GOAL := local

all: local docker
docker:
	docker build -t javasdk .
local:
	mvn clean compile dependency:copy-dependencies assembly:assembly