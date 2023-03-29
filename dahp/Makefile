all: build

.PHONY: build

build:
	mvn clean package
	/bin/echo -e '#!/bin/sh\nexec java -jar "\x240" "\x24@"' | cat - target/dahp-1.0-full.jar > dahp
	chmod +x dahp

clean:
	rm -f dahp target/dahp-1.0-full.jar
