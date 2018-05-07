.PHONY: clean

CFLAGS  := -Wall -std=c99 -o3
LD      := gcc
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS    := client server

all: ${APPS}

client: app-client.o common.o client.o
	${LD} -o $@ $^ ${LDLIBS}

server: app-server.o common.o server.o
	${LD} -o $@ $^ ${LDLIBS}

test-client: test-client.o common.o client.o
	${LD} -o $@ $^ ${LDLIBS}

test-server: test-server.o common.o server.o
	${LD} -o $@ $^ ${LDLIBS}
	
clean:
	rm -f *.o ${APPS}

