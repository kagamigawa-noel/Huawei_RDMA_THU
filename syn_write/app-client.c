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

#define N 100005

double lat[N], sum = 0.0, end_time, base;
int l_count = 0;
double tran = 0.0, get = 0.0,  mete_tran = 0.0, work = 0.0, into = 0.0;
double get_i[N], work_i[N], tran_i[N], mete_tran_i[N];
int flag = 0;

void recollection( struct request_active *rq )
{
	int r_id = ((ull)rq-(ull)rpl->pool)/sizeof(struct request_active);
	pthread_mutex_lock(&rpl->rpl_mutex);
	rpl->queue[rpl->front++] = r_id;
	rpl->count++;
	if( rpl->front >= app_buffer_size ) rpl->front -= app_buffer_size;
	rq->ed = elapse_sec();
	lat[l_count] = rq->ed-rq->st-commit_time;
	sum += lat[l_count];
	if( rq->task->tp == WRITE ){
		get_i[l_count] = rq->get-rq->st;
		work_i[l_count] = rq->tran-rq->get;
		tran_i[l_count] = rq->back-rq->tran;
		mete_tran_i[l_count] = rq->ed-rq->back-commit_time; 
		
		if(	get_i[l_count] >= 0 && get_i[l_count] <= 100000 ) get += rq->get-rq->st; 
		if( work_i[l_count] >= 0 && work_i[l_count] <= 100000 ) work += rq->tran-rq->get; 
		if( tran_i[l_count] >= 0 && tran_i[l_count] <= 100000 ) tran += rq->back-rq->tran; 
		if( mete_tran_i[l_count] >= 0 && mete_tran_i[l_count] <= 100000 ) mete_tran += rq->ed-rq->back-commit_time;
	}
	else{
		get_i[l_count] = rq->get-rq->st;
		work_i[l_count] = rq->tran-rq->get;
		tran_i[l_count] = rq->ed-rq->tran;
		
		if(	get_i[l_count] >= 0 && get_i[l_count] <= 100000 ) get += rq->get-rq->st; 
		if( work_i[l_count] >= 0 && work_i[l_count] <= 100000 ) work += rq->tran-rq->get; 
		if( tran_i[l_count] >= 0 && tran_i[l_count] <= 100000 ) tran += rq->ed-rq->tran; 
		into += rq->into-rq->get;
	}
	end_time = rq->ed;
	l_count ++;
	DEBUG("%.0lf %.0lf %.0lf\n", rq->st-base, rq->ed-base, rq->ed-rq->st);
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
	if( argc != 3 ){
		puts("error arg");
		exit(1);
	}
	int rq_sub = atoi(argv[2]);
	int i, j;
	base = elapse_sec();
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
	initialize_active( mpl->pool, app_buffer_size*request_size, argv[1] );
	fprintf(stderr, "BUFFER_SIZE %d recv_buffer_num %d buffer_per_size %d ctrl_number %d\n",\
		BUFFER_SIZE, recv_buffer_num, buffer_per_size, ctrl_number);
	if( BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number ) {
		fprintf(stderr, "BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number\n");
		exit(1);
	}
	double rq_start, rq_end;
	/* target 300000 */
	rq_start = elapse_sec()-base;
	for( i = 0; i < rq_sub; i ++ ){
		int r_id, m_id, sl_id;
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
		rpl->pool[r_id].st = elapse_sec();
		
		huawei_syn_send( &rpl->pool[r_id] );
		
		if( i == 0 ) usleep(15);
		// if( i%5 == 0 )
			// usleep(2);
		DEBUG("send request r %d m %d SL %d id %d\n", r_id, m_id, sl_id, i);
	}
	rq_end = elapse_sec()-base;
	sleep(test_time);
	
	finalize_active();
	printf("request count %d write count %d back count %d\n",\
	request_count, write_count, back_count);
	//printf("request count %d write count %d send package count %d\n",\
	request_count, write_count, send_package_count);
	printf("request start %lf end %lf back %lf interval %lf now %lf\n",\
	rq_start/1000.0, rq_end/1000.0, (end_time-base)/1000.0, (end_time-base-rq_start)/1000.0, (elapse_sec()-base)/1000.0);
	printf("average latency %lf total %d\n", sum/l_count, l_count);
	printf("into %lf\n\n", into/l_count);
	printf("get %lf\n", get/l_count);
	printf("work %lf\n", work/l_count);
	printf("tran %lf\n", tran/l_count);
	printf("mete_tran %lf\n", mete_tran/l_count);
	
	FILE *ind;
	ind = fopen( "individual", "w+" );
	for( i = 0; i < l_count; i ++ ){
		fprintf(  ind, "%05d %12.0lf %12.0lf %12.0lf %12.0lf %12.0lf\n",
		i, get_i[i], work_i[i], tran_i[i], mete_tran_i[i], lat[i]);
	}
	fclose( ind );
}