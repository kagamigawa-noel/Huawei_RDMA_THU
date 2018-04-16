#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>  
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>  

void print()
{
	printf("%d\n", rand());
}

int main()
{
	signal(SIGALRM, print); 
	struct itimerval reset_value;
	reset_value.it_value.tv_sec = 0;  
    reset_value.it_value.tv_usec = 1000000;  
    reset_value.it_interval.tv_sec = 0;  
    reset_value.it_interval.tv_usec = 1000000;  
	setitimer(ITIMER_REAL, &reset_value, NULL); -std=c99
	while(1);
}