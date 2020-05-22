JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
PATH:=${JAVA_HOME}/bin:${PATH}

LIBS=/home/cs-local-linux/339/hadoop-2.9.2/share/hadoop
NEW_CLASSPATH=${LIBS}/mapreduce/*:${LIBS}/common/*:${LIBS}/common/lib/*:lib/*:${CLASSPATH}

SRC = $(wildcard *.java)

all: build

build: ${SRC}
	${JAVA_HOME}/bin/javac -Xlint -classpath ${NEW_CLASSPATH} ${SRC}
	${JAVA_HOME}/bin/jar cvf build.jar *.class 

clean:
	rm -rf output/ intermediate/ *~ *.class
