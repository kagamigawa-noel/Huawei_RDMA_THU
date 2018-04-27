#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>  
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>  

int main()
{
	struct ibv_device **list;
	int num;
	list = ibv_free_device_list(num);
	printf("num %d \n", num);
	for( int i = 0; i < num; i ++ ){
		printf("%d: %s\n", i, list[i]->name);
	}
}