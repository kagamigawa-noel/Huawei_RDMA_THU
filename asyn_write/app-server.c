#include "common.h"

struct commit_buffer
{
	struct request_backup *buffer[80010];
	double time[80010];
	int front, tail, count, shutdown;
	pthread_mutex_t mutex;
};

ull data[300005];// !!!!注意大小
int num;
pthread_t working_id;
struct commit_buffer *cfr;
int commit_time = 0;// commit time 0us

double query;
double send_time, recv_time, cmt_time;
int recv_package, send_package_ack;

void *working()
{
	cfr = (struct commit_buffer *)malloc( sizeof(struct commit_buffer) );
	pthread_mutex_init(&cfr->mutex, NULL);
	cfr->front = cfr->tail = cfr->count = cfr->shutdown = 0;
	
	while( !cfr->shutdown ){
		int id;
		pthread_mutex_lock(&cfr->mutex);
		if( cfr->count >= 80000 ){
			fprintf(stderr, "commit buffer no space!!!\n");
			exit(1);
		}
		if( cfr->count == 0 ){
			pthread_mutex_unlock(&cfr->mutex);
			continue;
		}
		id = cfr->tail++;
		cfr->count --;
		if( cfr->tail >= 80000 ) cfr->tail -= 80000;
		pthread_mutex_unlock(&cfr->mutex);
		/* sth to deal with cfr->buffer[id] */
		//fprintf(stderr, "deal with request %p\n", cfr->buffer[id]);
		double tmp = cfr->time[id], now = elapse_sec();
		//printf("now %lf commit %lf gap %lf\n", now, tmp, now-tmp);
		if( tmp+commit_time-now > 10.0 ){
			usleep( (int)(tmp+commit_time-now-10.0) );
		}
		notify(cfr->buffer[id]);
	}
}

void solve( struct request_backup *rq )
{
	pthread_mutex_lock(&cfr->mutex);
	cfr->buffer[cfr->front] = rq;
	cfr->time[cfr->front++] = elapse_sec();
	if( cfr->front >= 80000 ) cfr->front -= 80000;
	cfr->count ++;
	pthread_mutex_unlock(&cfr->mutex);
	data[num++] = (ull)rq->private;
	//num ++;
	//fprintf(stderr, "commit request %llu\n", rq->private);
}

int cmp( const void *a, const void *b )
{
	return *(ull *)a > *(ull *)b ? 1 : -1;
}

int main()
{
	recv_package = 0;
	send_package_ack = 0;
	send_time = 0.0;
	recv_time = 0.0;
	cmt_time = 0.0;
	FILE *t1;
	t1 = fopen("1.txt", "w");
	if( t1 == NULL ) exit(1);
	initialize_backup( solve );
	fprintf(stderr, "BUFFER_SIZE %d recv_buffer_num %d buffer_per_size %d ctrl_number %d\n",\
		BUFFER_SIZE, recv_buffer_num, buffer_per_size, ctrl_number);
	if( BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number ) {
		fprintf(stderr, "BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number\n");
		exit(1);
	}
	pthread_create( &working_id, NULL, working, NULL );
	sleep(test_time);
	
	printf("xxx\n");
	cfr->shutdown = 1;
	pthread_cancel(working_id);
	pthread_join(working_id, NULL);
	finalize_backup();
	qsort( data, num, sizeof(ull), cmp );
	printf("recv num: %d\n", num);
	for( int i = 0; i < num-1; i ++ ){
		fprintf(t1, "%llu\n", data[i]);
	}
	printf("recv_package %d\n", recv_package);
	printf("send_package_ack %d\n", send_package_ack);
	printf("send_time %lf\n", send_time/1000.0);
	printf("recv_time %lf\n", recv_time/1000.0);
	printf("commit_time %lf\n", cmt_time/1000.0);
	// for( int i = 0; i < num; i ++ ){
		// printf("%llu ", data[i]);
		// if( i % 10 == 9 ) puts("");
	// }
	// puts("");
	fclose(t1);
}