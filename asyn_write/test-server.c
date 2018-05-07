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
	initialize_backup( solve );

	sleep(test_time);

}