benchmark: benchmark.c
	./env.sh
	gcc benchmark.c -L/usr/local/lib -lzmq -lczmq -g -std=gnu11 -o benchmark

clean:
	rm -f conf.h
	rm -f benchmark
