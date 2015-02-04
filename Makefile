
build:
	mvn package -Dmaven.test.skip=true

clean:
	rm -rf examples/*.log
	rm -rf *.log

s3:
	s3cmd put --recursive examples s3://pignlproc
	s3cmd put target/*.jar s3://pignlproc
