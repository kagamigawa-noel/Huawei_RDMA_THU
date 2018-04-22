.PHONY: clean

CFLAGS  := -Wall -std=c99 -g
LD      := gcc
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS    := client server

all: ${APPS}

client: app-client.o common.o client.o
	${LD} -o $@ $^ ${LDLIBS}

server: app-server.o common.o server.o
	${LD} -o $@ $^ ${LDLIBS}

clean:
	rm -f *.o ${APPS}

