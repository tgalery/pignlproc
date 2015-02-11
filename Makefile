
build:
	git clone --depth 1 https://github.com/dbpedia-spotlight/dbpedia-spotlight.git; \
	cd dbpedia-spotlight; \
	mvn -T 1C clean install; \
	cd .. ;\
	mvn package -Dmaven.test.skip=true; \

clean:
	rm -rf examples/*.log
	rm -rf *.log
