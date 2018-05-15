#include "common.h"

struct commit_buffer
{
	struct request_backup *buffer[20005];
	double time[20005];
	int front, tail, count, shutdown;
	pthread_mutex_t mutex;
	pthread_spinlock_t spin;
};

extern ull data[1<<15];
extern int num;
pthread_t working_id;
struct commit_buffer *cfr;
extern double ave_lat, base;
double dis = 0.0;

void *working()
{
	cfr = (struct commit_buffer *)malloc( sizeof(struct commit_buffer) );
#ifdef __MUTEX
	pthread_mutex_init(&cfr->mutex, NULL);
#else
	pthread_spin_init(&cfr->spin, NULL);
#endif
	cfr->front = cfr->tail = cfr->count = cfr->shutdown = 0;
	
	while( !cfr->shutdown ){
		int id;
#ifdef __MUTEX
		pthread_mutex_lock(&cfr->mutex);
#else
		pthread_spin_lock(&cfr->spin);
#endif

		if( cfr->count >= 20000 ){
			fprintf(stderr, "commit buffer no space!!!\n");
			exit(1);
		}
		if( cfr->count == 0 ){
#ifdef __MUTEX
			pthread_mutex_unlock(&cfr->mutex);
#else
			pthread_spin_unlock(&cfr->spin);
#endif	
			continue;
		}
		id = cfr->tail++;
		cfr->count --;
		if( cfr->tail >= 20000 ) cfr->tail -= 20000;
		
#ifdef __MUTEX
		pthread_mutex_unlock(&cfr->mutex);
#else
		pthread_spin_unlock(&cfr->spin);
#endif		
		
		/* sth to deal with cfr->buffer[id] */
		//fprintf(stderr, "deal with request %p\n", cfr->buffer[id]);
		double tmp = cfr->time[id], now = elapse_sec();
		//printf("now %lf commit %lf gap %lf\n", now, tmp, now-tmp);
		while( tmp+commit_time-elapse_sec() > 10 ){
			usleep( 1 );
		}
		dis += elapse_sec()-tmp-commit_time;
		notify(cfr->buffer[id]);
	}
}

void solve( struct request_backup *rq )
{
	
#ifdef __MUTEX
	pthread_mutex_lock(&cfr->mutex);
#else
	pthread_spin_lock(&cfr->spin);
#endif

	cfr->buffer[cfr->front] = rq;
	cfr->time[cfr->front++] = elapse_sec();
	if( cfr->front >= 20000 ) cfr->front -= 20000;
	cfr->count ++;
	
#ifdef __MUTEX
	pthread_mutex_unlock(&cfr->mutex);
#else
	pthread_spin_unlock(&cfr->spin);
#endif

	//data[num++] = (ull)rq->private;
	//fprintf(stderr, "commit request %p\n", rq);
	num ++;
	//notify(rq);
}

int cmp( const void *a, const void *b )
{
	return *(ull *)a > *(ull *)b ? 1 : -1;
}

int main()
{
	fprintf(stderr, "BUFFER_SIZE %d recv_buffer_num %d buffer_per_size %d ctrl_number %d\n",\
		BUFFER_SIZE, recv_buffer_num, buffer_per_size, ctrl_number);
	if( BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number ) {
		fprintf(stderr, "BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number\n");
		exit(1);
	}
	ave_lat = 0.0;
	pthread_create( &working_id, NULL, working, NULL );
	base = elapse_sec();
	initialize_backup( solve );
	sleep(test_time);
	
	cfr->shutdown = 1;
	//pthread_cancel(working_id);
	pthread_join(working_id, NULL);
	finalize_backup();
	//qsort( data, num, sizeof(ull), cmp );
	printf("recv num: %d\n", num);
	// for( int i = 0; i < num; i ++ ){
		// printf("%llu ", data[i]);
		// if( i % 10 == 9 ) puts("");
	// }
	// puts("");
	printf("ave_lat %lf\n", ave_lat/num);
	printf("dis %lf\n", dis/num);
}