#include "common.h"

int app_buffer_size = 65536;

struct request_pool
{
	struct request_active *pool;
	int *queue;
	int count, front, tail;
	pthread_mutex_t rpl_mutex;
};

struct memory_pool
{
	char *pool;
	int *queue;
	int count, front, tail;
	pthread_mutex_t mpl_mutex;
};

struct ScatterList_pool
{
	struct ScatterList *pool;
	int *queue;
	int count, front, tail;
	pthread_mutex_t SLpl_mutex;
};

extern int write_count, request_count, back_count, send_package_count;
struct request_pool *rpl;
struct memory_pool *mpl;
struct ScatterList_pool *SLpl;

void recollection( struct request_active *rq )
{
	pthread_mutex_lock(&rpl->rpl_mutex);
	rpl->queue[rpl->front++] = ((ull)rq-(ull)rpl->pool)/sizeof(struct request_active);
	rpl->count++;
	if( rpl->front >= app_buffer_size ) rpl->front -= app_buffer_size;
	pthread_mutex_unlock(&rpl->rpl_mutex);
	
	pthread_mutex_lock(&mpl->mpl_mutex);
	mpl->queue[mpl->front++] = ((ull)rq->sl->address-(ull)mpl->pool)/request_size;
	mpl->count ++;
	if( mpl->front >= app_buffer_size ) mpl->front -= app_buffer_size;
	pthread_mutex_unlock(&mpl->mpl_mutex);
	
	pthread_mutex_lock(&SLpl->SLpl_mutex);
	SLpl->queue[SLpl->front++] = ((ull)rq->sl-(ull)SLpl->pool)/sizeof(struct ScatterList);
	SLpl->count ++;
	if( SLpl->front >= app_buffer_size ) SLpl->front -= app_buffer_size;
	pthread_mutex_unlock(&SLpl->SLpl_mutex);
	//printf("%llu reback ok\n", rq->private);
}

int main(int argc, char **argv)
{
	int i, j;
	double base = elapse_sec();
	/* initialize region */
	rpl = ( struct request_pool * )malloc( sizeof(struct request_pool) );
	rpl->pool = ( struct request_active * )malloc(app_buffer_size*sizeof(struct request_active));
	rpl->queue = ( int * )malloc(app_buffer_size*sizeof(int));
	
	mpl = ( struct memory_pool * )malloc( sizeof(struct memory_pool) );
	mpl->pool = ( char * )malloc(app_buffer_size*request_size);
	mpl->queue = ( int * )malloc(app_buffer_size*sizeof(int));
	
	SLpl = ( struct ScatterList_pool * )malloc( sizeof(struct ScatterList_pool) );
	SLpl->pool = ( struct ScatterList * )malloc(app_buffer_size*sizeof(struct ScatterList));
	SLpl->queue = ( int * )malloc(app_buffer_size*sizeof(int));
	
	
	rpl->front = 0;
	rpl->tail = 0;
	rpl->count = 0;
	mpl->front = 0;
	mpl->tail = 0;
	mpl->count = 0;
	SLpl->front = 0;
	SLpl->tail = 0;
	SLpl->count = 0;
	pthread_mutex_init(&rpl->rpl_mutex, NULL);
	pthread_mutex_init(&mpl->mpl_mutex, NULL);
	pthread_mutex_init(&SLpl->SLpl_mutex, NULL);
	for( i = 0; i < app_buffer_size; i ++ ){
		rpl->count++;
		rpl->queue[rpl->front++] = i;
		if( rpl->front >= app_buffer_size ) rpl->front -= app_buffer_size;
		
		mpl->count++;
		mpl->queue[mpl->front++] = i;
		if( mpl->front >= app_buffer_size ) mpl->front -= app_buffer_size;
		
		SLpl->count++;
		SLpl->queue[SLpl->front++] = i;
		if( SLpl->front >= app_buffer_size ) SLpl->front -= app_buffer_size;
	}
	printf("local add: %p length: %d\n", mpl->pool, app_buffer_size*request_size);
	initialize_active( mpl->pool, app_buffer_size*request_size, argv[1], argv[2] );
	fprintf(stderr, "BUFFER_SIZE %d recv_buffer_num %d buffer_per_size %d ctrl_number %d\n",\
		BUFFER_SIZE, recv_buffer_num, buffer_per_size, ctrl_number);
	if( BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number ) {
		fprintf(stderr, "BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number\n");
		exit(1);
	}
	double rq_start, rq_end;
	/* target 300000 */
	rq_start = elapse_sec()-base;
	for( i = 0; i < 5000; i ++ ){
		int r_id, m_id, sl_id;
		r_id = m_id = sl_id = i;
		while(1){
			pthread_mutex_lock(&rpl->rpl_mutex);
			if( rpl->count == 0 ){
				pthread_mutex_unlock(&rpl->rpl_mutex);
				continue;
			}
			r_id = rpl->queue[rpl->tail++];
			if( rpl->tail >= app_buffer_size ) rpl->tail -= app_buffer_size;
			rpl->count --;
			pthread_mutex_unlock(&rpl->rpl_mutex);
			break;
		}
		
		while(1){
			pthread_mutex_lock(&mpl->mpl_mutex);
			if( mpl->count == 0 ){
				pthread_mutex_unlock(&mpl->mpl_mutex);
				continue;
			}
			m_id = mpl->queue[mpl->tail++];
			if( mpl->tail >= app_buffer_size ) mpl->tail -= app_buffer_size;
			mpl->count --;
			pthread_mutex_unlock(&mpl->mpl_mutex);
			break;
		}
		
		while(1){
			pthread_mutex_lock(&SLpl->SLpl_mutex);
			if( SLpl->count == 0 ){
				pthread_mutex_unlock(&SLpl->SLpl_mutex);
				continue;
			}
			sl_id = SLpl->queue[SLpl->tail++];
			if( SLpl->tail >= app_buffer_size ) SLpl->tail -= app_buffer_size;
			SLpl->count --;
			pthread_mutex_unlock(&SLpl->SLpl_mutex);
			break;
		}
		
		SLpl->pool[sl_id].next = NULL;
		SLpl->pool[sl_id].address = mpl->pool+request_size*m_id;
		SLpl->pool[sl_id].length = request_size;
		
		rpl->pool[r_id].private = (ull)i;
		rpl->pool[r_id].sl = &SLpl->pool[sl_id];
		rpl->pool[r_id].callback = recollection;
		
		huawei_syn_send( &rpl->pool[r_id] );
		//fprintf(stderr, "send request r %d m %d SL %d id %d\n", r_id, m_id, sl_id, i);
	}
	rq_end = elapse_sec()-base;
	sleep(test_time);
	
	finalize_active();
	printf("request count %d write count %d back count %d\n",\
	request_count, write_count, back_count);
	//printf("request count %d write count %d send package count %d\n",\
	request_count, write_count, send_package_count);
	printf("request start %lf end %lf now %lf\n",\
	rq_start/1000.0, rq_end/1000.0, (elapse_sec()-base)/1000.0);
}