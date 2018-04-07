#include "common.h"
#define REQUEST_BUFFER_SIZE 4096

const int thread_number = 1;
const int request_size = 4;//B
const int connect_number = 16;
const int scatter_size = 1;
const int package_size = 2;
const int resend_limit = 5;
const int send_buffer_per_size = 3*20;

struct request_buffer
{
	struct request_active *buffer[8192];
	int count, front, tail; 
};

struct task_pool
{
	struct task_active pool[8192];
	uint bit[8192/32];
};

struct scatter_pool
{
	struct scatter_active pool[8192];
	uint bit[8192/32];
};

struct package_pool
{
	struct package_active pool[8192];
	uint bit[8192/32];
};

struct rdma_addrinfo *addr;
pthread_mutex_t mutex0, mutex1, task_mutex[4], scatter_mutex[4], rdma_mutex[4], rbf_mutex;
pthread_cond_t cond0, cond1;
struct request_buffer *rbf;
struct task_pool *tpl;
struct scatter_pool *spl;
struct package_pool *ppl;
pthread_t completion_id, pthread_id[5];
int tmp[5];

void initialize_active( void *address, int length, char *ip_address, char *ip_port );
int on_event(struct rdma_cm_event *event, int tid);
void *working_thread(void *arg);
void *completion_active();
void huawei_send( struct request_active *rq );

int on_event(struct rdma_cm_event *event, int tid)
{
	int r = 0;
    if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)
        r = on_addr_resolved(event->id, tid);
    else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)
      r = on_route_resolved(event->id, tid);
    else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
      r = on_connection(event->id, tid);
	// else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
	  // r = on_disconnect(event->id);
	else
	  die("on_event: unknown event.");

	return r;
}

void initialize_active( void *address, int length, char *ip_address, char *ip_port )
{
	end = 0;
	memgt = ( struct memory_management * ) malloc( sizeof( struct memory_management ) );
	qpmgt = ( struct qp_management * ) malloc( sizeof( struct qp_management ) );
	memgt->application.next = NULL;
	memgt->application.address = address;
	memgt->application.length = length;
	
	struct ibv_wc wc;
	for( int i = 0; i < connect_number; i ++ ){
		if( i == 0 ){
			TEST_NZ(rdma_getaddrinfo(ip_address, ip_port, NULL, &addr));
		}
		else{
			post_recv( 0, i, 0 );
			//printf("post recv ok\n");
			TEST_NZ( get_wc( &wc ) );
			
			char port[20];
			fprintf(stderr, "port: %d\n", *((int *)(memgt->recv_buffer)));
			int tmp = *((int *)(memgt->recv_buffer));
			sprintf(port, "%d\0", tmp);
			TEST_NZ(rdma_getaddrinfo(ip_address, port, NULL, &addr));
		}
		TEST_Z(ec = rdma_create_event_channel());
		TEST_NZ(rdma_create_id(ec, &conn_id[i], NULL, RDMA_PS_TCP));
		TEST_NZ(rdma_resolve_addr(conn_id[i], NULL, addr->ai_dst_addr, TIMEOUT_IN_MS));
		rdma_freeaddrinfo(addr);
		while (rdma_get_cm_event(ec, &event) == 0) {
			struct rdma_cm_event event_copy;
			memcpy(&event_copy, event, sizeof(*event));
			rdma_ack_cm_event(event);
			if (on_event(&event_copy, i)){
				break;
			}
		}
		fprintf(stderr, "build connect succeed %d\n", i);
	}
	
	post_recv( 0, 20, 0 );
	TEST_NZ( get_wc( &wc ) );
	
	memcpy( &memgt->peer_mr, memgt->recv_buffer, sizeof(struct ibv_mr) );
	printf("peer add: %p length: %d\n", memgt->peer_mr.addr,
	memgt->peer_mr.length);
	
	for( int i = 0; i < connect_number; i ++ ){
		post_recv( i, i, 0 );
	}
	
	/*initialize request_buffer*/
	fprintf(stderr, "initialize request_buffer begin\n");
	rbf = ( struct request_buffer * )malloc( sizeof( struct request_buffer * ) );
	pthread_mutex_init(&rbf_mutex, NULL);
	rbf->count = 0;
	rbf->front = 0;
	rbf->tail = 0;
	fprintf(stderr, "initialize request_buffer end\n");
	
	/*initialize pool*/
	fprintf(stderr, "initialize pool begin\n");
	tpl = ( struct task_pool * ) malloc( sizeof( struct task_pool ) );
	spl = ( struct scatter_pool * ) malloc( sizeof( struct scatter_pool ) );
	ppl = ( struct package_pool * ) malloc( sizeof( struct package_pool ) );
	memset( tpl->bit, 0, sizeof(tpl->bit) );
	memset( spl->bit, 0, sizeof(spl->bit) );
	memset( ppl->bit, 0, sizeof(ppl->bit) );
	fprintf(stderr, "initialize pool end\n");
	
	/*create pthread pool*/
	fprintf(stderr, "create pthread pool begin\n");
	pthread_create( &completion_id, NULL, completion_active, NULL );
	
	pthread_mutex_init(&mutex0, NULL);
	pthread_mutex_init(&mutex1, NULL);
	pthread_cond_init(&cond0, NULL);
	pthread_cond_init(&cond1, NULL);
	
	for( int i = 0; i < thread_number; i ++ ){
		pthread_mutex_init(&task_mutex[i], NULL);
		pthread_mutex_init(&scatter_mutex[i], NULL);
	}
	
	for( int i = 0; i < thread_number; i ++ ){
		tmp[i] = i;
		pthread_mutex_init(&rdma_mutex[i], NULL);
		pthread_create( &pthread_id[i], NULL, working_thread, &tmp[i] );
	}
	fprintf(stderr, "create pthread pool end\n");
}

/*
active:

rdma_write(scatter) information
wr_id: scatter_pointer (64bit)
imm_data: no

send(package) information
wr_id: package_pointer (64bit)
imm_data: no

recv(package) information
wr_id: qp_id
imm_data: package id in pool ( from remote )

backup:

send(package) information
wr_id: 0
imm_data: package id in pool

recv(package) information
wr_id: qp_id
imm_data: no

包： package_active pool下标+packge number+data
size: 4+4+20*package_size
*/

void *working_thread(void *arg)
{
	int thread_id = (*(int *)arg), i, j, cnt = 0, t_pos, s_pos, m_pos, qp_num, count = 0;
	qp_num = connect_number/thread_number;
	struct request_active *now;
	struct task_active *task_buffer[10];
	fprintf(stderr, "working thread #%d ready\n", thread_id);
	while(1){
		pthread_mutex_lock(&rbf_mutex);
		while( rbf->count == 0 ){
			pthread_cond_wait( &cond0, &rbf_mutex );
		}
		rbf->count --;
		now = rbf->buffer[rbf->tail++];
		
		//fprintf(stderr, "working thread #%d solve request %p\n", thread_id, now);
		pthread_mutex_unlock(&rbf_mutex);
		/* signal api */
		pthread_cond_signal( &cond1 );
		
		pthread_mutex_lock( &task_mutex[thread_id] );
		t_pos = query_bit_free( tpl->bit, 8192/thread_number*thread_id, 8192/thread_number );
		pthread_mutex_unlock( &task_mutex[thread_id] );
		
		/* initialize task_active */
		tpl->pool[t_pos].request = now;
		tpl->pool[t_pos].state = 0;
		//fprintf(stderr, "working thread #%d now %p request %p\n", thread_id, now, tpl->pool[t_pos].request);
		
		task_buffer[cnt++] = &tpl->pool[t_pos];
		
		if( cnt == scatter_size ){
			pthread_mutex_lock( &scatter_mutex[thread_id] );
			s_pos = query_bit_free( spl->bit, 8192/thread_number*thread_id, 8192/thread_number );
			pthread_mutex_unlock( &scatter_mutex[thread_id] );
			
			spl->pool[s_pos].number = cnt;
			for( j = 0; j < cnt; j ++ ){
				spl->pool[s_pos].task[j] = task_buffer[j];
				task_buffer[j]->state = 1;
				// can add sth to calculate memory size
			}
			
			spl->pool[s_pos].remote_sge.next = NULL;
			
			pthread_mutex_lock( &rdma_mutex[thread_id] );
			m_pos = query_bit_free( memgt->peer_bit, RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number*thread_id, \
			RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number );
			pthread_mutex_unlock( &rdma_mutex[thread_id] );
			//在不回收的情况下每个thread可传RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number 
			
			spl->pool[s_pos].remote_sge.address = memgt->peer_mr.addr+m_pos*( request_size*scatter_size );
			spl->pool[s_pos].remote_sge.length = request_size*cnt;//先假设每次只传一个int
			
			while( qp_query(thread_id*qp_num+count%qp_num) != 3 ){
				count ++;
			}
			int tmp_qp_id = thread_id*qp_num+count%qp_num;
			count ++;
			spl->pool[s_pos].qp_id = tmp_qp_id;
			
			spl->pool[s_pos].resend_count = 1;
			
			post_rdma_write( tmp_qp_id, &spl->pool[s_pos] );
			//fprintf(stderr, "working thread #%d submit scatter %04d\n", thread_id, s_pos);
			//fprintf(stderr, "working thread #%d submit task %04d scatter %04d remote %04d data %d\n", \
			thread_id, t_pos, s_pos, m_pos, *(int *)spl->pool[s_pos].task[0]->request->sl->address);
			
			cnt = 0;
		}
		
	}
}

void *completion_active()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc; wc = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc) );
	void *ctx;
	struct scatter_active *buffer[128];
	int i, cnt = 0, count = 0;
	int data[128];
	while(1){
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));
		while(ibv_poll_cq(cq, 1, wc)){
			// if( wc->status != IBV_WC_SUCCESS ){
				// fprintf(stderr, "wr_id: %lld wrong status %d type: ", wc->wr_id, wc->status);
				// switch (wc->opcode) {
					// case IBV_WC_RECV_RDMA_WITH_IMM: fprintf(stderr, "IBV_WC_RECV_RDMA_WITH_IMM\n"); break;
					// case IBV_WC_RDMA_WRITE: fprintf(stderr, "IBV_WC_RDMA_WRITE"); break;
					// case IBV_WC_RDMA_READ: fprintf(stderr, "IBV_WC_RDMA_READ\n"); break;
					// case IBV_WC_SEND: fprintf(stderr, "IBV_WC_SEND\n"); break;
					// case IBV_WC_RECV: fprintf(stderr, "IBV_WC_RECV\n"); break;
					// default : fprintf(stderr, "unknwon\n"); break;
				// }
			// }
			if( wc->opcode == IBV_WC_RDMA_WRITE ){
				struct scatter_active *now;
				now = ( struct scatter_active * )wc->wr_id;
				if( wc->status != IBV_WC_SUCCESS ){
					//printf("id: %lld qp: %d\n", ((ull)now-(ull)spl->pool)/sizeof(struct scatter_active), now->qp_id);
					if( now->resend_count >= resend_limit ){
						fprintf(stderr, "scatter %p wrong after resend %d times\n", now, now->resend_count);
					}
					else{
						now->resend_count ++;
						while( qp_query(count%connect_number) != 3 ){
							count ++;
						}
						now->qp_id = count%connect_number;
						count ++;
						
						post_rdma_write( now->qp_id, now );
						//fprintf(stderr, "completion thread resubmit scatter %p #%d\n", now, now->resend_count);
					}
					continue;
				}
				
				buffer[cnt++] = now;
				//fprintf(stderr, "get CQE scatter %p\n", now);
				fprintf(stderr, "get CQE scatter %lld\n", ((ull)now-(ull)spl->pool)/sizeof(struct scatter_active));
				for( i = 0; i < now->number; i ++ ){
					now->task[i]->state = 2;
					/*operate request callback*/
					//task_buffer[j]->request->callback();
				}
				
				if( cnt == package_size ){
					/* initialize package_active */
					int pos = query_bit_free( ppl->bit, 0, 8192 );
					ppl->pool[pos].number = cnt;
					ppl->pool[pos].resend_count = 1;
					for( i = 0; i < ppl->pool[pos].number; i ++ ){
						ppl->pool[pos].scatter[i] = buffer[i];
					}
					
					/* initialize send ack buffer */
					int send_pos = query_bit_free( memgt->send_bit, 0, \
					BUFFER_SIZE/send_buffer_per_size ); 
					ppl->pool[pos].send_buffer_id = send_pos;
					
					while( qp_query(count%connect_number) != 3 ){
						count ++;
					}
					
					send_package( &ppl->pool[pos], pos, \
					send_buffer_per_size*send_pos,  count%connect_number);
					
					count ++;
					
					//fprintf(stderr, "submit package %p id %d\n", &ppl->pool[pos], pos);
					fprintf(stderr, "submit package id %d send id %d\n", pos, send_pos);
					
					cnt = 0;
				}
			}
			
			if( wc->opcode == IBV_WC_SEND ){
				struct package_active *now;
				now = ( struct package_active * )wc->wr_id;
				if( wc->status != IBV_WC_SUCCESS ){
					if( now->resend_count >= resend_limit ){
						fprintf(stderr, "package %p wrong after resend %d times\n", now, now->resend_count);
					}
					else{
						now->resend_count ++;
						int pos = ((ull)now-(ull)ppl->pool)/sizeof( struct package_active );
						while( qp_query(count%connect_number) != 3 ){
							count ++;
						}
						
						send_package( now, pos, \
						send_buffer_per_size*now->send_buffer_id, count%connect_number);
						
						count ++;
						fprintf(stderr, "submit package id %d send id %d #%d\n", \
						pos, now->send_buffer_id, now->resend_count);
					}
					continue;
				}
				
				for( int i = 0; i < now->number; i ++ ){
					for( int j = 0; j < now->scatter[i]->number; j ++ ){
						now->scatter[i]->task[j]->state = 3;
					}
				}
				
				/* clean send buffer */
				data[0] = now->send_buffer_id;
				update_bit( memgt->send_bit, 0, \
				BUFFER_SIZE/send_buffer_per_size, data, 1  );
			}
			
			if( wc->opcode == IBV_WC_RECV ){
				if( qp_query(wc->wr_id) == 3 )
					post_recv( wc->wr_id, wc->wr_id, 0 );
				else continue;
				
				struct package_active *now;
				now = &ppl->pool[wc->imm_data];
				
				fprintf(stderr, "get CQE package id %d\n", wc->imm_data);
				
				for( int i = 0; i < now->number; i ++ ){
					for( int j = 0; j < now->scatter[i]->number; j ++ ){
						now->scatter[i]->task[j]->state = 4;
					}
				}
				
				/* 回收空间 + 修改peer_bit */
				int num = 0;
				
				/* clean task pool*/
				for( int i = 0; i < now->number; i ++ ){
					int s_pos = ( (ull)now->scatter[i]-(ull)spl->pool )/sizeof( struct scatter_active );
					num = 0;
					for( int j = 0; j < now->scatter[i]->number; j ++ ){
						data[num++] = ( (ull)now->scatter[i]->task[j] - (ull)tpl->pool )/sizeof(struct task_active);
					}
					s_pos /= 8192/thread_number;
					
					pthread_mutex_lock(&task_mutex[s_pos]);
					update_bit( tpl->bit, 8192/thread_number*s_pos, 8192/thread_number, \
						data, num );
					pthread_mutex_unlock(&task_mutex[s_pos]);
				}
				
				/* clean peer bit */
				for( int i = 0; i < now->number; i ++ ){
					int s_pos = ( (ull)now->scatter[i]-(ull)spl->pool )/sizeof( struct scatter_active );
					s_pos /= 8192/thread_number;
					data[0] = ((ull)now->scatter[i]->remote_sge.address-(ull)memgt->peer_bit)\
					/(RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number);
					
					pthread_mutex_lock(&rdma_mutex[s_pos]);
					update_bit( memgt->peer_bit, RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number*s_pos,\
						RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number, data, 1 );
					pthread_mutex_unlock(&rdma_mutex[s_pos]);
				}
				
				/* clean scatter pool */
				for( int i = 0; i < now->number; i ++ ){
					int s_pos = ( (ull)now->scatter[i]-(ull)spl->pool )/sizeof( struct scatter_active );
					data[0] = s_pos;
					s_pos /= 8192/thread_number;
					
					pthread_mutex_lock(&task_mutex[s_pos]);
					update_bit( spl->bit, 8192/thread_number*s_pos, 8192/thread_number, \
						data, 1 );
					pthread_mutex_unlock(&task_mutex[s_pos]);
				}
				
				/* clean package pool */
				data[0] = wc->imm_data;
				update_bit( ppl->bit, 0, 8192, data, 1 );
			}
		}
	}
}

void huawei_send( struct request_active *rq )
{
	pthread_mutex_lock(&rbf_mutex);
	while( rbf->count == REQUEST_BUFFER_SIZE ){	
		pthread_cond_wait( &cond1, &rbf_mutex );
	}
	rbf->buffer[rbf->front++] = rq;
	rbf->count ++;

	pthread_mutex_unlock(&rbf_mutex);
	
	/* signal working thread */
	pthread_cond_signal( &cond0 );

	return ;
}

struct request_active rq[1000];
struct ScatterList sl[1000];

int main(int argc, char **argv)
{
	char *add;
	int len;
	add = ( char * )malloc( 8192 );
	len = 8192;
	printf("local add: %p length: %d\n", add, len);
	initialize_active( add, len, argv[1], argv[2] );
	// printf("scatter size %d\n", sizeof( struct scatter_active ) );
	// for( int i = 0; i < 10; i ++ ) printf("%d: %p\n", i, &spl->pool[i]);
	sleep(2);
	void *ct; int i, j;
	ct = add;
	for( i = 0; i < 64; i ++ ){
		*( int * )ct = i;
		sl[i].next = NULL;
		sl[i].address = ct;
		sl[i].length = sizeof(int);
		ct += sizeof(int);
		
		rq[i].sl = &sl[i];
		//printf("request #%02d submit %p num_add: %p data %d\n", i, &rq[i],\
		rq[i].sl->address, *(int *)rq[i].sl->address);
		huawei_send( &rq[i] );
	}
	sleep(10);
}
