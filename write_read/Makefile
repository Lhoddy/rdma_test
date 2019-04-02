.PHONY: clean

CFLAGS  := -Wall -Werror -g
LDFLAGS := ${LDFLAGS} -lrdmacm -libverbs -lpthread

APPS    := rdma-client rdma-server

all: ${APPS}

rdma-client: client.o rdma_common.h
	cc -o $@ $^ ${LDFLAGS}

rdma-server: server.o rdma_common.h
	cc -o $@ $^ ${LDFLAGS}

clean:
	rm -f *.o ${APPS}
ac:
	rm -f *.o ${APPS};rm *.c
