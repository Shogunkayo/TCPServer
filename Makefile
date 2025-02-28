all: serial parallel
	echo "Made all"

test_serial: serial
	./bin/serial_server 8080 > /dev/null &
	PARALLEL=0 ./test.sh
	killall serial_server

serial: src/serial/main.cpp
	if test -d bin; then echo exists; else mkdir bin; fi
	g++ -std=c++11 -o bin/serial_server src/serial/main.cpp 

serial_debug: src/serial/main.cpp
	if test -d bin; then echo exists; else makdir bin; fi
	g++ -std=c++11 -o bin/serial_server src/serial/main.cpp -D DEBUG

test_parallel: parallel
	./bin/parallel_server 8080 > /dev/null &
	PARALLEL=1 ./test.sh
	killall parallel_server

parallel: src/parallel/main.cpp
	if test -d bin; then echo exists; else mkdir bin; fi
	g++ -std=c++11 -o bin/parallel_server -pthread -fno-stack-protector src/parallel/main.cpp

parallel_debug: src/parallel/main.cpp
	if test -d bin; then echo exists; else mkdir bin; fi
	g++ -std=c++11 -o bin/parallel_server -pthread -fno-stack-protector src/parallel/main.cpp -D DEBUG

clean:
	rm -rf bin

test: test_serial test_parallel
	echo "Made test"
