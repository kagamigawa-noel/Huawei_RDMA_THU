#include "common.h"

struct commit_buffer
{
	struct request_backup *buffer[8192];
	double time[8192];
	int front, tail, count, shutdown;
	pthread_mutex_t mutex;
};

extern ull data[1<<15];
extern int num = 0;
pthread_t working_id;
struct commit_buffer *cfr;
int commit_time = 0;// commit time 0us

void *working()
{
	cfr = (struct commit_buffer *)malloc( sizeof(struct commit_buffer) );
	pthread_mutex_init(&cfr->mutex, NULL);
	cfr->front = cfr->tail = cfr->count = cfr->shutdown = 0;
	
	while( !cfr->shutdown ){
		int id;
		pthread_mutex_lock(&cfr->mutex);
		if( cfr->count >= 8192 ){
			fprintf(stderr, "commit buffer no space!!!\n");
			exit(1);
		}
		id = cfr->tail++;
		if( cfr->tail >= 8192 ) cfr->tail -= 8192;
		pthread_mutex_unlock(&cfr->mutex);
		/* sth to deal with cfr->buffer[id] */
		double tmp = cfr->time[id], now = elapse_sec();
		if( now - tmp - commit_time > 5.0 ){
			usleep( (int)(now-tmp-5.0) );
		}
		notify(cfr->buffer[id]);
	}
}

void solve( struct request_backup *rq )
{
	pthread_mutex_lock(&cfr->mutex);
	cfr->buffer[cfr->front] = rq;
	cfr->time[cfr->front++] = elapse_sec();
	if( cfr->front >= 8192 ) cfr->front -= 8192;
	pthread_mutex_unlock(&cfr->mutex);
	data[num++] = (ull)rq->private;
	//fprintf(stderr, "commit request %p\n", rq);
}

int main()
{
	initialize_backup( solve );
	fprintf(stderr, "BUFFER_SIZE %d recv_buffer_num %d buffer_per_size %d ctrl_number %d\n",\
		BUFFER_SIZE, recv_buffer_num, buffer_per_size, ctrl_number);
	if( BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number ) {
		fprintf(stderr, "BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number\n");
		exit(1);
	}
	pthread_create( &working_id, NULL, working, NULL );
	sleep(10);
	
	cfr->shutdown = 1;
	pthread_join(working_id, NULL)
	finalize_backup();
	qsort( data, num, sizeof(ull), cmp );
	printf("recv num: %d\n", num);
	for( int i = 0; i < num; i ++ ){
		printf("%llu ", data[i]);
		if( i % 9 == 0 ) puts("");
	}
	puts("");
}