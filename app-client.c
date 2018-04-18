#include "common.h"

struct request_pool
{
	struct request_active pool[8192];
	int queue[8192];
	int count, front, tail;
	pthread_mutex_t rpl_mutex;
};

struct memory_pool
{
	char pool[8192*1024*4];
	int queue[8192];
	int count, front, tail;
	pthread_mutex_t mpl_mutex;
};

struct ScatterList_pool
{
	struct ScatterList pool[8192];
	int queue[8192];
	int count, front, tail;
	pthread_mutex_t SLpl_mutex;
};

extern int send_package_count, write_count, request_count;
struct request_pool *rpl;
struct memory_pool *mpl;
struct ScatterList_pool *SLpl;

void recollection( struct request_active *rq )
{
	pthread_mutex_lock(&rpl->rpl_mutex);
	rpl->queue[rpl->front++] = ((ull)rq-(ull)rpl->pool)/sizeof(struct request_active);
	rpl->count++;
	if( rpl->front >= 8192 ) rpl->front -= 8192;
	pthread_mutex_unlock(&rpl->rpl_mutex);
	
	pthread_mutex_lock(&mpl->mpl_mutex);
	mpl->queue[mpl->front++] = ((ull)rq->sl->address-(ull)mpl->pool)/(4*1024);
	mpl->count ++;
	if( mpl->front >= 8192 ) mpl->front -= 8192;
	pthread_mutex_unlock(&mpl->mpl_mutex);
	
	pthread_mutex_lock(&SLpl->SLpl_mutex);
	SLpl->queue[SLpl->front++] = ((ull)rq->sl-(ull)SLpl->pool)/sizeof(struct ScatterList);
	SLpl->count ++;
	if( SLpl->front >= 8192 ) SLpl->front -= 8192;
	pthread_mutex_unlock(&SLpl->SLpl_mutex);
	printf("%llu reback ok\n", rq->private);
}

int main(int argc, char **argv)
{
	int i, j;
	/* initialize region */
	rpl = ( struct request_pool * )malloc( sizeof(struct request_pool) );
	mpl = ( struct memory_pool * )malloc( sizeof(struct memory_pool) );
	SLpl = ( struct ScatterList_pool * )malloc( sizeof(struct ScatterList_pool) );
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
	for( i = 0; i < 8192; i ++ ){
		rpl->count++;
		rpl->queue[rpl->front++] = i;
		if( rpl->front >= 8192 ) rpl->front -= 8192;
		
		mpl->count++;
		mpl->queue[mpl->front++] = i;
		if( mpl->front >= 8192 ) mpl->front -= 8192;
		
		SLpl->count++;
		SLpl->queue[SLpl->front++] = i;
		if( SLpl->front >= 8192 ) SLpl->front -= 8192;
	}
	printf("local add: %p length: %d\n", mpl->pool, 8192*1024*4);
	initialize_active( mpl->pool, 8192*1024*4, argv[1], argv[2] );
	fprintf(stderr, "BUFFER_SIZE %d recv_buffer_num %d buffer_per_size %d ctrl_number %d\n",\
		BUFFER_SIZE, recv_buffer_num, buffer_per_size, ctrl_number);
	if( BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number ) {
		fprintf(stderr, "BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number\n");
		exit(1);
	}
	sleep(3);
	
	for( i = 0; i < 100; i ++ ){
		int r_id, m_id, sl_id;
		while(1){
			pthread_mutex_lock(&rpl->rpl_mutex);
			if( rpl->count == 0 ){
				pthread_mutex_unlock(&rpl->rpl_mutex);
				continue;
			}
			r_id = rpl->queue[rpl->tail++];
			if( rpl->tail >= 8192 ) rpl->tail -= 8192;
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
			if( mpl->tail >= 8192 ) mpl->tail -= 8192;
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
			if( SLpl->tail >= 8192 ) SLpl->tail -= 8192;
			SLpl->count --;
			pthread_mutex_unlock(&SLpl->SLpl_mutex);
			break;
		}
		
		SLpl->pool[sl_id].next = NULL;
		SLpl->pool[sl_id].address = mpl->pool+4*1024*m_id;
		SLpl->pool[sl_id].length = 4*1024;
		
		rpl->pool[r_id].private = (ull)i;
		rpl->pool[r_id].sl = &SLpl->pool[sl_id];
		rpl->pool[r_id].callback = recollection;
		
		huawei_send( &rpl->pool[r_id] );
		fprintf(stderr, "send request r %d m %d SL %d\n", r_id, m_id, sl_id);
	}
	sleep(10);
	
	finalize_active();
	printf("request count %d write count %d send package count %d\n",\
	request_count, write_count, send_package_count);
}