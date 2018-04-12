#include "common.h"
#define REQUEST_BUFFER_SIZE 4096

struct request_buffer
{
	struct request_active *buffer[8192];
	int count, front, tail; 
	pthread_mutex_t rbf_mutex;
};

struct task_pool
{
	struct task_active pool[8192];
	uint bit[8192/32];
	pthread_mutex_t task_mutex[4];
};

struct scatter_pool
{
	struct scatter_active pool[8192];
	uint bit[8192/32];
	pthread_mutex_t scatter_mutex[4];
};

struct package_pool
{
	struct package_active pool[8192];
	uint bit[8192/32];
};

struct thread_pool
{
	int number, tmp[5];
	pthread_mutex_t mutex0, mutex1;
	pthread_cond_t cond0, cond1;
	pthread_t completion_id, pthread_id[5];
};

struct rdma_addrinfo *addr;
struct request_buffer *rbf;
struct task_pool *tpl;
struct scatter_pool *spl;
struct package_pool *ppl;
struct thread_pool *thpl;
int send_package_count, write_count;

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
	
	for( int i = qpmgt->data_num; i < qpmgt->data_num+qpmgt->ctrl_num; i ++ ){
		for( int j = 0; j < recv_buffer_num; j ++ )
			post_recv( i, (i-qpmgt->data_num)*recv_buffer_num+j, 0 );
	}
	
	/* test sth */
	send_package_count = 0;
	write_count = 0;
	
	/*initialize request_buffer*/
	fprintf(stderr, "initialize request_buffer begin\n");
	rbf = ( struct request_buffer * )malloc( sizeof( struct request_buffer ) );
	pthread_mutex_init(&rbf->rbf_mutex, NULL);
	rbf->count = 0;
	rbf->front = 0;
	rbf->tail = 0;
	fprintf(stderr, "initialize request_buffer end\n");
	
	/*initialize pool*/
	fprintf(stderr, "initialize pool begin\n");
	tpl = ( struct task_pool * ) malloc( sizeof( struct task_pool ) );
	spl = ( struct scatter_pool * ) malloc( sizeof( struct scatter_pool ) );
	ppl = ( struct package_pool * ) malloc( sizeof( struct package_pool ) );
	thpl = ( struct thread_pool * ) malloc( sizeof( struct thread_pool ) );
	memset( tpl->bit, 0, sizeof(tpl->bit) );
	memset( spl->bit, 0, sizeof(spl->bit) );
	memset( ppl->bit, 0, sizeof(ppl->bit) );
	
	for( int i = 0; i < thread_number; i ++ ){
		pthread_mutex_init(&tpl->task_mutex[i], NULL);
		pthread_mutex_init(&spl->scatter_mutex[i], NULL);
		pthread_mutex_init(&memgt->rdma_mutex[i], NULL);
	}
	
	fprintf(stderr, "initialize pool end\n");
	
	/*create pthread pool*/
	fprintf(stderr, "create pthread pool begin\n");
	pthread_create( &thpl->completion_id, NULL, completion_active, NULL );
	
	pthread_mutex_init(&thpl->mutex0, NULL);
	pthread_mutex_init(&thpl->mutex1, NULL);
	pthread_cond_init(&thpl->cond0, NULL);
	pthread_cond_init(&thpl->cond1, NULL);
	
	thpl->number = thread_number;
	
	for( int i = 0; i < thread_number; i ++ ){
		thpl->tmp[i] = i;
		pthread_create( &thpl->pthread_id[i], NULL, working_thread, &thpl->tmp[i] );
	}
	fprintf(stderr, "create pthread pool end\n");
}

void finalize_active()
{
	
}

void *working_thread(void *arg)
{
	int thread_id = (*(int *)arg), i, j, cnt = 0, t_pos, s_pos, m_pos, qp_num, count = 0;
	qp_num = qpmgt->data_num/thread_number;
	struct request_active *now;
	struct task_active *task_buffer[10];
	fprintf(stderr, "working thread #%d ready\n", thread_id);
	//sleep(5);
	while(1){
		pthread_mutex_lock(&rbf->rbf_mutex);
		//fprintf(stderr, "working thread #%d lock\n", thread_id);
		while( rbf->count <= 0 ){
			pthread_cond_wait( &thpl->cond0, &rbf->rbf_mutex );
		}
		rbf->count --;				
		now = rbf->buffer[rbf->tail++];
		if( rbf->tail >= REQUEST_BUFFER_SIZE ) rbf->tail -= REQUEST_BUFFER_SIZE;

		//fprintf(stderr, "working thread #%d solve request %p\n", thread_id, now);
		pthread_mutex_unlock(&rbf->rbf_mutex);
		/* signal api */
		pthread_cond_signal( &thpl->cond1 );
		
		pthread_mutex_lock( &tpl->task_mutex[thread_id] );
		t_pos = query_bit_free( tpl->bit, 8192/thread_number*thread_id, 8192/thread_number );
		pthread_mutex_unlock( &tpl->task_mutex[thread_id] );
		
		/* initialize task_active */
		tpl->pool[t_pos].request = now;
		tpl->pool[t_pos].state = 0;
		fprintf(stderr, "working thread #%d request %p r_id %llu task %d\n",\
		thread_id, tpl->pool[t_pos].request, tpl->pool[t_pos].request->private, t_pos);
		
		task_buffer[cnt++] = &tpl->pool[t_pos];
		
		if( cnt == scatter_size ){
			//pthread_mutex_lock( &spl->scatter_mutex[thread_id] );
			s_pos = query_bit_free( spl->bit, 8192/thread_number*thread_id, 8192/thread_number );
			//pthread_mutex_unlock( &spl->scatter_mutex[thread_id] );
			
			spl->pool[s_pos].number = cnt;
			for( j = 0; j < cnt; j ++ ){
				spl->pool[s_pos].task[j] = task_buffer[j];
				task_buffer[j]->state = 1;
				// can add sth to calculate memory size
			}
			
			spl->pool[s_pos].remote_sge.next = NULL;
			
			pthread_mutex_lock( &memgt->rdma_mutex[thread_id] );
			m_pos = query_bit_free( memgt->peer_bit, RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number*thread_id, \
			RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number );
			pthread_mutex_unlock( &memgt->rdma_mutex[thread_id] );
			//在不回收的情况下每个thread可传RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number 
			
			spl->pool[s_pos].remote_sge.address = memgt->peer_mr.addr+m_pos*( request_size*scatter_size );
			spl->pool[s_pos].remote_sge.length = request_size*cnt;//先假设每次只传一个int
			void *start = spl->pool[s_pos].remote_sge.address;
			for( j = 0; j < cnt; j ++ ){
				spl->pool[s_pos].task[j]->remote_sge.address = start;
				spl->pool[s_pos].task[j]->remote_sge.length = \
				spl->pool[s_pos].task[j]->request->sl->length;
				start += spl->pool[s_pos].task[j]->remote_sge.length;
			}
			
			while( qp_query(thread_id*qp_num+count%qp_num) != 3 ){
				count ++;
			}
			int tmp_qp_id = thread_id*qp_num+count%qp_num;
			count ++;
			spl->pool[s_pos].qp_id = tmp_qp_id;
			spl->pool[s_pos].resend_count = 1;
			
			post_rdma_write( tmp_qp_id, &spl->pool[s_pos] );
			//fprintf(stderr, "working thread #%d submit scatter %04d qp: %d %d\n",\
			thread_id, s_pos, tmp_qp_id, *(int *)spl->pool[s_pos].task[i]->request->sl->address);
			// for( int i = 0; i < spl->pool[s_pos].number; i ++ ){
				// fprintf(stderr, " %d", *(int *)spl->pool[s_pos].task[i]->request->sl->address );
			// }
			// fprintf(stderr, "\n");
			// fprintf(stderr, "\n");
			fprintf(stderr, "working thread #%d submit scatter %04d qp %02d remote %04d\n", \
			thread_id, s_pos, tmp_qp_id, m_pos);
			
			usleep(work_timeout);
			cnt = 0;
		}
		
	}
}

void *completion_active()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	struct scatter_active *buffer[1024];
	int i, j, k, cnt = 0, count = 0, num;
	int data[128];
	fprintf(stderr, "completion thread ready\n");
	while(1){
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));
		// if( cq == s_ctx->cq_data ) puts("cq_data");
		// else if( cq == s_ctx->cq_ctrl ) puts("cq_ctrl");
		// else puts("NULL");
		int tot = 0;
		while(1){
			num = ibv_poll_cq(cq, 100, wc_array);
			if( num <= 0 ) break;
			tot += num;
			fprintf(stderr, "%04d CQE get!!!\n", num);
			for( k = 0; k < num; k ++ ){
				wc = &wc_array[k];
				// switch (wc->opcode) {
					// case IBV_WC_RECV_RDMA_WITH_IMM: fprintf(stderr, "IBV_WC_RECV_RDMA_WITH_IMM\n"); break;
					// case IBV_WC_RDMA_WRITE: fprintf(stderr, "IBV_WC_RDMA_WRITE\n"); break;
					// case IBV_WC_RDMA_READ: fprintf(stderr, "IBV_WC_RDMA_READ\n"); break;
					// case IBV_WC_SEND: fprintf(stderr, "IBV_WC_SEND\n"); break;
					// case IBV_WC_RECV: fprintf(stderr, "IBV_WC_RECV\n"); break;
					// default : fprintf(stderr, "unknwon\n"); break;
				// }
				if( wc->opcode == IBV_WC_RDMA_WRITE ){
					struct scatter_active *now;
					now = ( struct scatter_active * )wc->wr_id;
					if( wc->status != IBV_WC_SUCCESS ){
						//printf("id: %lld qp: %d\n", ((ull)now-(ull)spl->pool)/sizeof(struct scatter_active), now->qp_id);
						if( now->resend_count >= resend_limit ){
							fprintf(stderr, "scatter %d wrong after resend %d times\n", \
							((ull)now-(ull)spl->pool)/sizeof(struct scatter_active), now->resend_count);
							// can add sth to avoid the death of this scatter
						}
						else{
							now->resend_count ++;
							while( qp_query(count%qpmgt->data_num) != 3 ){
								count ++;
							}
							now->qp_id = count%qpmgt->data_num;
							count ++;
							
							post_rdma_write( now->qp_id, now );
							fprintf(stderr, "completion thread resubmit scatter %d #%d\n", \
							((ull)now-(ull)spl->pool)/sizeof(struct scatter_active), now->resend_count);
						}
						continue;
					}
					write_count ++;
					buffer[cnt++] = now;
					//fprintf(stderr, "get CQE scatter %p\n", now);
					//fprintf(stderr, "get CQE scatter %lld\n", ((ull)now-(ull)spl->pool)/sizeof(struct scatter_active));
					fprintf(stderr, "get CQE scatter %04lld\n", ((ull)now-(ull)spl->pool)/sizeof(struct scatter_active));
					for( i = 0; i < now->number; i ++ ){
						now->task[i]->state = 2;
						/*operate request callback*/
						//task_buffer[j]->request->callback();
					}
					
					if( cnt == package_size ){
						/* initialize package_active */
						int pos = query_bit_free( ppl->bit, 0, 8192 );
						//fprintf(stderr, "pos %04d\n", pos);
						ppl->pool[pos].number = cnt;
						ppl->pool[pos].resend_count = 1;
						for( i = 0; i < ppl->pool[pos].number; i ++ ){
							ppl->pool[pos].scatter[i] = buffer[i];
						}
						
						/* initialize send ack buffer */
						int send_pos = query_bit_free( memgt->send_bit, 0, \
						BUFFER_SIZE/buffer_per_size ); 
						ppl->pool[pos].send_buffer_id = send_pos;
						
						while( qp_query(count%qpmgt->ctrl_num+qpmgt->data_num) != 3 ){
							count ++;
						}
						
						send_package( &ppl->pool[pos], pos, \
						buffer_per_size*send_pos, count%qpmgt->ctrl_num+qpmgt->data_num);
						
						count ++;
						
						//fprintf(stderr, "submit package %p id %d\n", &ppl->pool[pos], pos);
						fprintf(stderr, "submit package id %04d send id %04d\n", pos, send_pos);
						
						cnt = 0;
					}
				}
				
				if( wc->opcode == IBV_WC_SEND ){
					struct package_active *now;
					now = ( struct package_active * )wc->wr_id;
					if( wc->status != IBV_WC_SUCCESS ){
						if( now->resend_count >= resend_limit ){
							fprintf(stderr, "package %d wrong after resend %d times\n", \
							((ull)now-(ull)ppl->pool)/sizeof(struct package_active), now->resend_count);
						}
						else{
							now->resend_count ++;
							int pos = ((ull)now-(ull)ppl->pool)/sizeof( struct package_active );
							while( qp_query(count%qpmgt->ctrl_num+qpmgt->data_num) != 3 ){
								count ++;
							}
							
							send_package( now, pos, \
							buffer_per_size*now->send_buffer_id, count%qpmgt->ctrl_num+qpmgt->data_num);
							
							count ++;
							fprintf(stderr, "submit package id %d send id %d #%d\n", \
							pos, now->send_buffer_id, now->resend_count);
						}
						continue;
					}
					fprintf(stderr, "send package %04d success\n", \
					((ull)now-(ull)ppl->pool)/sizeof( struct package_active ));
					send_package_count ++;
					
					for( int i = 0; i < now->number; i ++ ){
						for( int j = 0; j < now->scatter[i]->number; j ++ ){
							now->scatter[i]->task[j]->state = 3;
						}
					}
					
					/* clean send buffer */
					data[0] = now->send_buffer_id;
					update_bit( memgt->send_bit, 0, \
					BUFFER_SIZE/buffer_per_size, data, 1  );
				}
				
				if( wc->opcode == IBV_WC_RECV ){
					if( qp_query(wc->wr_id/recv_buffer_num+qpmgt->data_num) == 3 )
						post_recv( wc->wr_id/recv_buffer_num+qpmgt->data_num,\
					 wc->wr_id, 0 );
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
					int num = 0, reback = 0;
					
					/* clean task pool*/
					for( int i = 0; i < now->number; i ++ ){
						int s_pos = ( (ull)now->scatter[i]-(ull)spl->pool )/sizeof( struct scatter_active );
						num = 0;
						for( int j = 0; j < now->scatter[i]->number; j ++ ){
							data[num++] = ( (ull)now->scatter[i]->task[j] - (ull)tpl->pool )/sizeof(struct task_active);
						}
						s_pos /= 8192/thread_number;
						
						pthread_mutex_lock(&tpl->task_mutex[s_pos]);
						reback = update_bit( tpl->bit, 8192/thread_number*s_pos, 8192/thread_number, \
							data, num );
						pthread_mutex_unlock(&tpl->task_mutex[s_pos]);
					}
					//fprintf(stderr, "clean task pool %d\n", reback);
					
					/* clean peer bit */
					for( int i = 0; i < now->number; i ++ ){
						int s_pos = ( (ull)now->scatter[i]-(ull)spl->pool )/sizeof( struct scatter_active );
						s_pos /= 8192/thread_number;
						data[0] = ((ull)now->scatter[i]->remote_sge.address-(ull)memgt->peer_mr.addr)\
						/(scatter_size*request_size);
						
						pthread_mutex_lock(&memgt->rdma_mutex[s_pos]);
						reback = update_bit( memgt->peer_bit, RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number*s_pos,\
							RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number, data, 1 );
						pthread_mutex_unlock(&memgt->rdma_mutex[s_pos]);
					}
					//fprintf(stderr, "clean peer bit %d\n", reback);
					
					/* clean scatter pool */
					for( int i = 0; i < now->number; i ++ ){
						int s_pos = ( (ull)now->scatter[i]-(ull)spl->pool )/sizeof( struct scatter_active );
						data[0] = s_pos;
						s_pos /= 8192/thread_number;
						
						pthread_mutex_lock(&tpl->task_mutex[s_pos]);
						reback = update_bit( spl->bit, 8192/thread_number*s_pos, 8192/thread_number, \
							data, 1 );
						pthread_mutex_unlock(&tpl->task_mutex[s_pos]);
					}
					//fprintf(stderr, "clean scatter pool %d\n", reback);
					
					/* clean package pool */
					data[0] = wc->imm_data;
					update_bit( ppl->bit, 0, 8192, data, 1 );
				}
			}
			if( tot >= 150 ){ tot = 0; break; }
		}
	}
}

void huawei_send( struct request_active *rq )
{
	pthread_mutex_lock(&rbf->rbf_mutex);
	while( rbf->count == REQUEST_BUFFER_SIZE ){	
		pthread_cond_wait( &thpl->cond1, &rbf->rbf_mutex );
	}
	rbf->buffer[rbf->front++] = rq;
	if( rbf->front >= REQUEST_BUFFER_SIZE ) rbf->front -= REQUEST_BUFFER_SIZE;
	rbf->count ++;

	pthread_mutex_unlock(&rbf->rbf_mutex);
	
	/* signal working thread */
	pthread_cond_signal( &thpl->cond0 );

	return ;
}

struct request_active rq[1005];
struct ScatterList sl[1005];

int main(int argc, char **argv)
{
	char *add;
	int len;
	add = ( char * )malloc( 4096*1005 );
	len = 4096*1005;
	printf("local add: %p length: %d\n", add, len);
	initialize_active( add, len, argv[1], argv[2] );
	fprintf(stderr, "BUFFER_SIZE %d recv_buffer_num %d buffer_per_size %d ctrl_number %d\n",\
		BUFFER_SIZE, recv_buffer_num, buffer_per_size, ctrl_number);
	if( BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number ) {
		fprintf(stderr, "BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number\n");
		exit(1);
	}
	sleep(5);
	void *ct; int i, j;
	ct = add;
	for( i = 0; i < 1000; i ++ ){
		*( int * )ct = i;
		j = i;
		sl[j].next = NULL;
		sl[j].address = ct;
		sl[j].length = sizeof(4096);
		ct += sizeof(4096);
		
		rq[j].sl = &sl[j];
		rq[j].private = (ull)j;
		printf("request #%02d submit %p num_add: %p r_id %llu\n", i, &rq[j],\
		rq[j].sl->address, rq[j].private);
		huawei_send( &rq[j] );
	}
	sleep(15);
	// for( i = 0; i < thread_number; i ++ ){
		// TEST_NZ(pthread_cancel(pthread_id[i]));
		// TEST_NZ(pthread_join(pthread_id[i], NULL));
	// }
	// TEST_NZ(pthread_cancel(completion_id));
	// TEST_NZ(pthread_join(completion_id, NULL));
	for( i = 0; i < connect_number; i ++ ){
		fprintf(stderr, "qp: %d state %d\n", i, qp_query(i));
	}
	printf("write count %d send package count %d\n", write_count, send_package_count);
	exit(1);
}
