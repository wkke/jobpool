NAME="jobpool-engine"
VERSION="1.0.0.x86_64"

OS_NAME=$(strip $(if $(uname -s), $(uname -s), $(OS)))

.PHONY: all clean build docker-build

all: clean build docker-build

clean: ;@echo "clean history data..."; \
	rm -f doc/buid/jobpool

build: ;@echo "compile and build..."; \
	echo "download mod"; \
	go mod download; \
	echo "clean mod"; \
	go mod tidy;	\
	echo "copy mod to vendor"; \
	go mod vendor; \
	echo "start compiling and building"; \
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build; \
	cp jobpool doc/build/ ;

docker-build: ;@echo "build docker image..."; \
  	echo "${NAME}:${VERSION}"; \
  	cd doc/build; \
  	docker rmi -f ${NAME}:${VERSION}; \
  	docker build -t ${NAME}:${VERSION} . ; \

