#include "common.h"

struct request_buffer
{
	struct request_active **buffer;
	int count, front, tail; 
	pthread_mutex_t rbf_mutex;
};

struct scatter_buffer
{
	struct scatter_active **buffer;
	int cnt, shutdown;
	double start;
	pthread_t signal_id;
	pthread_mutex_t sbf_mutex, signal_mutex;
	pthread_cond_t signal_cond;
};

struct task_pool
{
	struct task_active *pool;
	uint *bit;
	pthread_mutex_t task_mutex[4];
};

struct scatter_pool
{
	struct scatter_active *pool;
	uint *bit;
	pthread_mutex_t scatter_mutex[4];
};

struct package_pool
{
	struct package_active *pool;
	uint *bit;
	pthread_mutex_t package_mutex;
};

struct thread_pool
{
	int number, tmp[5], shutdown;
	pthread_mutex_t mutex0, mutex1;
	pthread_cond_t cond0, cond1;
	pthread_t completion_id, pthread_id[5];
};

struct rdma_addrinfo *addr;
struct request_buffer *rbf;
struct scatter_buffer *sbf;
struct task_pool *tpl;
struct scatter_pool *spl;
struct package_pool *ppl;
struct thread_pool *thpl;
int send_package_count, write_count, request_count;
struct timeval test_start;
extern double get_working, do_working, \
cq_send, cq_recv, cq_write, cq_waiting, cq_poll, q_task, other;

void initialize_active( void *address, int length, char *ip_address, char *ip_port );
void finalize_active();
int on_event(struct rdma_cm_event *event, int tid);
void *working_thread(void *arg);
void *completion_active();
void huawei_send( struct request_active *rq );
void *full_time_send();
int clean_send_buffer( struct package_active *now );
int clean_package( struct package_active *now );

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

int clean_scatter( struct scatter_active *now )
{
	int data[20], num = 0, reback = 0;
	/* 回收空间 + 修改peer_bit */
	/* clean task pool*/
	int s_pos = ( (ull)now-(ull)spl->pool )/sizeof( struct scatter_active );
	s_pos /= scatter_pool_size/thread_number;
	num = 0;
	for( int j = 0; j < now->number; j ++ ){
		data[num++] = ( (ull)now->task[j] - (ull)tpl->pool )/sizeof(struct task_active);
	}
	//pthread_mutex_lock(&tpl->task_mutex[s_pos]);
	reback = update_bit( tpl->bit, task_pool_size/thread_number*s_pos, task_pool_size/thread_number, \
		data, num );
	if( reback != 0 ){
		fprintf(stderr, "update task_pool failure %d\n", reback);
		exit(1);
	}
	//pthread_mutex_unlock(&tpl->task_mutex[s_pos]);
	//fprintf(stderr, "clean task pool %d\n", reback);
	
	/* clean peer bit */
	data[0] = ((ull)now->remote_sge.address-(ull)memgt->peer_mr.addr)\
	/(scatter_size*request_size);
	
	//pthread_mutex_lock(&memgt->rdma_mutex[s_pos]);
	reback = update_bit( memgt->peer_bit, RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number*s_pos,\
		RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number, data, 1 );
	if( reback != 0 ){
		fprintf(stderr, "update remote_region failure %d\n", reback);
		exit(1);
	}
	//pthread_mutex_unlock(&memgt->rdma_mutex[s_pos]);
	//fprintf(stderr, "clean peer bit %d\n", reback);
	
	/* clean scatter pool */
	data[0] = ( (ull)now-(ull)spl->pool )/sizeof( struct scatter_active );
	//pthread_mutex_lock(&spl->scatter_mutex[s_pos]);
	reback = update_bit( spl->bit, scatter_pool_size/thread_number*s_pos, scatter_pool_size/thread_number, \
		data, 1 );
	if( reback != 0 ){
		fprintf(stderr, "update scatter_pool failure %d\n", reback);
		exit(1);
	}
	//pthread_mutex_unlock(&spl->scatter_mutex[s_pos]);
	//fprintf(stderr, "clean scatter pool %d\n", reback);
	
	return 0;
}

int clean_send_buffer( struct package_active *now )
{
	int data[10];
	data[0] = now->send_buffer_id;
	pthread_mutex_lock(&memgt->send_mutex);
	update_bit( memgt->send_bit, 0, \
	BUFFER_SIZE/buffer_per_size, data, 1  );
	pthread_mutex_unlock(&memgt->send_mutex);
	return 0;
}

int clean_package( struct package_active *now )
{
	int data[128], num = 0, reback = 0;
	/* 回收空间 + 修改peer_bit */
	
	for( int i = 0; i < now->number; i ++ ){
		clean_scatter(now->scatter[i]);
	}
	/* clean package pool */
	data[0] = ( (ull)now-(ull)ppl->pool )/(sizeof(struct package_active));
	//pthread_mutex_lock(&ppl->package_mutex);
	update_bit( ppl->bit, 0, package_pool_size, data, 1 );
	//pthread_mutex_unlock(&ppl->package_mutex);
	
	return 0;
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
		for( int j = 0; j < recv_imm_data_num; j ++ )
			post_recv( i, (i-qpmgt->data_num)*recv_imm_data_num+j, 0 );
	}
	
	/* test sth */
	send_package_count = 0;
	write_count = 0;
	request_count = 0;
	
	/*initialize request_buffer*/
	fprintf(stderr, "initialize request_buffer begin\n");
	rbf = ( struct request_buffer * )malloc( sizeof( struct request_buffer ) );
	pthread_mutex_init(&rbf->rbf_mutex, NULL);
	rbf->count = 0;
	rbf->front = 0;
	rbf->tail = 0;
	rbf->buffer = ( struct request_active ** )malloc(sizeof(struct request_active *)*request_buffer_size);
	fprintf(stderr, "initialize request_buffer end\n");
	
	/*initialize scatter_buffer*/
	fprintf(stderr, "initialize scatter_buffer begin\n");
	sbf = ( struct scatter_buffer * )malloc( sizeof( struct scatter_buffer ) );
	pthread_mutex_init(&sbf->sbf_mutex, NULL);
	pthread_mutex_init(&sbf->signal_mutex, NULL);
	pthread_cond_init(&sbf->signal_cond, NULL);
	sbf->cnt = 0;
	sbf->shutdown = 0;
	sbf->buffer = ( struct scatter_active ** )malloc(sizeof(struct scatter_active *)*scatter_buffer_size);
	pthread_create( &sbf->signal_id, NULL, full_time_send, NULL );
	fprintf(stderr, "initialize scatter_buffer end\n");
	
	/*initialize pool*/
	fprintf(stderr, "initialize pool begin\n");
	tpl = ( struct task_pool * ) malloc( sizeof( struct task_pool ) );
	spl = ( struct scatter_pool * ) malloc( sizeof( struct scatter_pool ) );
	ppl = ( struct package_pool * ) malloc( sizeof( struct package_pool ) );
	thpl = ( struct thread_pool * ) malloc( sizeof( struct thread_pool ) );
	
	tpl->pool = ( struct task_active * )malloc( sizeof(struct task_active)*task_pool_size );
	spl->pool = ( struct scatter_active * )malloc( sizeof(struct scatter_active)*scatter_pool_size );
	ppl->pool = ( struct package_active * )malloc( sizeof(struct package_active)*package_pool_size );
	
	tpl->bit = ( uint * )malloc( sizeof(uint)*task_pool_size/32 );
	spl->bit = ( uint * )malloc( sizeof(uint)*scatter_pool_size/32 );
	ppl->bit = ( uint * )malloc( sizeof(uint)*package_pool_size/32 );
	
	memset( tpl->bit, 0, sizeof(tpl->bit) );
	memset( spl->bit, 0, sizeof(spl->bit) );
	memset( ppl->bit, 0, sizeof(ppl->bit) );
	
	for( int i = 0; i < thread_number; i ++ ){
		pthread_mutex_init(&tpl->task_mutex[i], NULL);
		pthread_mutex_init(&spl->scatter_mutex[i], NULL);
		pthread_mutex_init(&memgt->rdma_mutex[i], NULL);
	}
	pthread_mutex_init(&ppl->package_mutex, NULL);
	pthread_mutex_init(&memgt->send_mutex, NULL);
	fprintf(stderr, "initialize pool end\n");
	
	/*create pthread pool*/
	fprintf(stderr, "create pthread pool begin\n");
	pthread_create( &thpl->completion_id, NULL, completion_active, NULL );
	
	pthread_mutex_init(&thpl->mutex0, NULL);
	pthread_mutex_init(&thpl->mutex1, NULL);
	pthread_cond_init(&thpl->cond0, NULL);
	pthread_cond_init(&thpl->cond1, NULL);
	
	thpl->number = thread_number;
	thpl->shutdown = 0;
	
	for( int i = 0; i < thread_number; i ++ ){
		thpl->tmp[i] = i;
		pthread_create( &thpl->pthread_id[i], NULL, working_thread, &thpl->tmp[i] );
	}
	fprintf(stderr, "create pthread pool end\n");
}

void finalize_active()
{
	/* destroy pthread pool */
	if( !thpl->shutdown ){
		thpl->shutdown = 1;
		pthread_cond_broadcast(&thpl->cond0);
		for( int i = 0; i < thpl->number; i ++ ){
			//TEST_NZ(pthread_cancel(thpl->pthread_id[i]));
			TEST_NZ(pthread_join(thpl->pthread_id[i], NULL));
		}
		TEST_NZ(pthread_cancel(thpl->completion_id));
		TEST_NZ(pthread_join(thpl->completion_id, NULL));
		
		TEST_NZ(pthread_mutex_destroy(&thpl->mutex0));
		TEST_NZ(pthread_mutex_destroy(&thpl->mutex1));
		TEST_NZ(pthread_cond_destroy(&thpl->cond0));
		TEST_NZ(pthread_cond_destroy(&thpl->cond1));
		
		free(thpl); thpl = NULL;
	}
	fprintf(stderr, "destroy pthread pool success\n");
	
	/* destroy request buffer */
	TEST_NZ(pthread_mutex_destroy(&rbf->rbf_mutex));
	free(rbf->buffer); rbf->buffer = NULL;
	free(rbf); rbf = NULL;
	fprintf(stderr, "destroy request buffer success\n");
	
	/* destroy scatter buffer */
	sbf->shutdown = 1;
	usleep(full_time_interval);
	TEST_NZ(pthread_mutex_destroy(&sbf->sbf_mutex));
	TEST_NZ(pthread_mutex_destroy(&sbf->signal_mutex));
	TEST_NZ(pthread_cond_destroy(&sbf->signal_cond));
	TEST_NZ(pthread_join(sbf->signal_id, NULL));
	free(sbf->buffer); sbf->buffer = NULL;
	free(sbf); sbf = NULL;	
	fprintf(stderr, "destroy scatter buffer success\n");
	
	/* destroy task pool */
	for( int i = 0; i < thread_number; i ++ ){
		TEST_NZ(pthread_mutex_destroy(&tpl->task_mutex[i]));
	}
	free(tpl->pool); tpl->pool = NULL;
	free(tpl->bit); tpl->bit = NULL;
	free(tpl); tpl = NULL;
	fprintf(stderr, "destroy task pool success\n");
	
	/* destroy scatter pool */
	for( int i = 0; i < thread_number; i ++ ){
		TEST_NZ(pthread_mutex_destroy(&spl->scatter_mutex[i]));
	}
	free(spl->pool); spl->pool = NULL;
	free(spl->bit); spl->bit = NULL;
	free(spl); spl = NULL;
	fprintf(stderr, "destroy scatter pool success\n");
	
	/* destroy package pool */
	TEST_NZ(pthread_mutex_destroy(&ppl->package_mutex));
	free(ppl->pool); ppl->pool = NULL;
	free(ppl->bit); ppl->bit = NULL;
	free(ppl); ppl = NULL;
	fprintf(stderr, "destroy package pool success\n");
	
	/* destroy qp management */
	destroy_qp_management();
	fprintf(stderr, "destroy qp management success\n");
	
	/* destroy memory management */
	destroy_memory_management(end);
	fprintf(stderr, "destroy memory management success\n");
		
	/* destroy connection struct */
	destroy_connection();
	fprintf(stderr, "destroy connection success\n");
	
	fprintf(stderr, "finalize end\n");
}

void *working_thread(void *arg)
{
	int thread_id = (*(int *)arg), i, j, cnt = 0, t_pos, s_pos, m_pos, qp_num, count = 0;
	qp_num = qpmgt->data_num/thread_number;
	struct request_active *now, *request_buffer[10];
	struct task_active *task_buffer[10];
	fprintf(stderr, "working thread #%d ready\n", thread_id);
	//sleep(5);
	while(1){
		double tmp_time = elapse_sec();
		pthread_mutex_lock(&rbf->rbf_mutex);
		//fprintf(stderr, "working thread #%d lock\n", thread_id);
		while( rbf->count <= 0 && !thpl->shutdown ){
			pthread_cond_wait( &thpl->cond0, &rbf->rbf_mutex );
		}
		if( thpl->shutdown ){
			pthread_mutex_unlock(&rbf->rbf_mutex);
			pthread_exit(0);
		}
		while( rbf->count > 0 && cnt < scatter_size ){
			rbf->count --;				
			request_buffer[cnt++] = rbf->buffer[rbf->tail++];
			if( rbf->tail >= request_buffer_size ) rbf->tail -= request_buffer_size;
		}
		request_count += cnt;
		//fprintf(stderr, "working thread #%d solve request %p\n", thread_id, now);
		pthread_mutex_unlock(&rbf->rbf_mutex);
		/* signal api */
		pthread_cond_signal( &thpl->cond1 );
		
		get_working += elapse_sec()-tmp_time;
		tmp_time = elapse_sec();
		
		for( i = 0; i < cnt; i ++ ){
			//pthread_mutex_lock( &tpl->task_mutex[thread_id] );
			t_pos = query_bit_free( tpl->bit, task_pool_size/thread_number*thread_id, task_pool_size/thread_number );
			if( t_pos==-1 ){
				fprintf(stderr, "no more space while finding task_pool\n");
				exit(1);
			}
			//pthread_mutex_unlock( &tpl->task_mutex[thread_id] );
			
			/* initialize task_active */
			tpl->pool[t_pos].request = request_buffer[i];
			tpl->pool[t_pos].state = 0;
			fprintf(stderr, "working thread #%d request %p r_id %llu task %d\n",\
			thread_id, tpl->pool[t_pos].request, tpl->pool[t_pos].request->private, t_pos);
			
			task_buffer[i] = &tpl->pool[t_pos];
		}
		
		q_task += elapse_sec()-tmp_time;
		tmp_time = elapse_sec();
		
		//pthread_mutex_lock( &spl->scatter_mutex[thread_id] );
		s_pos = query_bit_free( spl->bit, scatter_pool_size/thread_number*thread_id, scatter_pool_size/thread_number );
		if( s_pos==-1 ){
			fprintf(stderr, "no more space while finding scatter_pool\n");
			exit(1);
		}
		//pthread_mutex_unlock( &spl->scatter_mutex[thread_id] );
		
		spl->pool[s_pos].number = cnt;
		for( j = 0; j < cnt; j ++ ){
			spl->pool[s_pos].task[j] = task_buffer[j];
			task_buffer[j]->state = 1;
			// can add sth to calculate memory size
		}
		
		spl->pool[s_pos].remote_sge.next = NULL;
		
		//printf("scale %d\n", RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number*thread_id);
		while(1){
			//pthread_mutex_lock( &memgt->rdma_mutex[thread_id] );
			m_pos = query_bit_free( memgt->peer_bit, RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number*thread_id, \
			RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number );
			if( m_pos==-1 ){
				//fprintf(stderr, "no more space while finding remote_region\n");
				//exit(1);
				//pthread_mutex_unlock( &memgt->rdma_mutex[thread_id] );
				usleep(waiting_time);
				continue;
			}
			//pthread_mutex_unlock( &memgt->rdma_mutex[thread_id] );
			break;
		}
		//在不回收的情况下每个thread可传RDMA_BUFFER_SIZE/scatter_size/request_size/thread_number 
		
		spl->pool[s_pos].remote_sge.address = memgt->peer_mr.addr+m_pos*( request_size*scatter_size );
		spl->pool[s_pos].remote_sge.length = request_size*cnt;
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
		
		//usleep(work_timeout);
		cnt = 0;
		
		other += elapse_sec()-tmp_time;
	}
}

void *completion_active()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	int i, j, k, count = 0, num;
	int data[128];
	struct scatter_active *buffer[128];
	fprintf(stderr, "completion thread ready\n");
	sbf->start = elapse_sec();
	pthread_cond_signal( &sbf->signal_cond );
	fprintf(stderr, "full time clock start\n");
	while(1){
		double tmp_time = elapse_sec();
		
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));
		
		cq_waiting += elapse_sec()-tmp_time;
		// if( cq == s_ctx->cq_data ) puts("cq_data");
		// else if( cq == s_ctx->cq_ctrl ) puts("cq_ctrl");
		// else puts("NULL");
		int tot = 0;
		while(1){
			double tmp_time = elapse_sec();
			num = ibv_poll_cq(cq, 100, wc_array);
			cq_poll += elapse_sec()-tmp_time;
			
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
					double tmp_time = elapse_sec();
					
					struct scatter_active *now;
					now = ( struct scatter_active * )wc->wr_id;
					if( wc->status != IBV_WC_SUCCESS ){
						//printf("id: %lld qp: %d\n", ((ull)now-(ull)spl->pool)/sizeof(struct scatter_active), now->qp_id);
						if( now->resend_count >= resend_limit ){
							fprintf(stderr, "scatter %d wrong after resend %d times\n", \
							((ull)now-(ull)spl->pool)/sizeof(struct scatter_active), now->resend_count);
							// can add sth to avoid the death of this scatter
							for( int i = 0; i < now->number; i ++ ){
								now->task[i]->state = -1;
								fprintf(stderr, "request %llu failure\n", \
								now->task[i]->request->private);
							}
							clean_scatter(now);
						}
						else{
							now->resend_count ++;
							while( re_qp_query(count%qpmgt->data_num) != 3 ){
								count ++;
							}
							now->qp_id = count%qpmgt->data_num;
							count ++;
							
							post_rdma_write( now->qp_id, now );
							fprintf(stderr, "completion thread resubmit scatter %d #%d wa %d\n", \
							((ull)now-(ull)spl->pool)/sizeof(struct scatter_active), now->resend_count, wc->status);
						}
						continue;
					}
					write_count ++;
					pthread_mutex_lock(&sbf->sbf_mutex);
					sbf->buffer[sbf->cnt++] = now;
					pthread_mutex_unlock(&sbf->sbf_mutex);
					//fprintf(stderr, "get CQE scatter %lld\n", ((ull)now-(ull)spl->pool)/sizeof(struct scatter_active));
					fprintf(stderr, "get CQE scatter %04lld\n", ((ull)now-(ull)spl->pool)/sizeof(struct scatter_active));
					for( i = 0; i < now->number; i ++ ){
						now->task[i]->state = 2;
						/*operate request callback*/
						now->task[i]->request->callback(now->task[i]->request);
					}
					
					pthread_mutex_lock(&sbf->sbf_mutex);
					if( sbf->cnt == package_size ){
						sbf->start = elapse_sec();
						int num = sbf->cnt;
						sbf->cnt = 0;
						for( i = 0; i < num; i ++ ) buffer[i] = sbf->buffer[i];
						pthread_mutex_unlock(&sbf->sbf_mutex);
						
						/* initialize package_active */
						//pthread_mutex_lock(&ppl->package_mutex);
						int pos = query_bit_free( ppl->bit, 0, package_pool_size );
						if( pos==-1 ){
							fprintf(stderr, "no more space while finding package_pool\n");
							exit(1);
						}
						//pthread_mutex_unlock(&ppl->package_mutex);
						//fprintf(stderr, "pos %04d\n", pos);
						ppl->pool[pos].number = num;
						ppl->pool[pos].resend_count = 1;
						for( i = 0; i < ppl->pool[pos].number; i ++ ){
							ppl->pool[pos].scatter[i] = buffer[i];
						}
						
						/* initialize send ack buffer */
						pthread_mutex_lock(&memgt->send_mutex);
						int send_pos = query_bit_free( memgt->send_bit, 0, \
						BUFFER_SIZE/buffer_per_size ); 
						if( send_pos==-1 ){
							fprintf(stderr, "no more space while finding send_buffer\n");
							exit(1);
						}
						pthread_mutex_unlock(&memgt->send_mutex);
						
						ppl->pool[pos].send_buffer_id = send_pos;
						
						while( qp_query(count%qpmgt->ctrl_num+qpmgt->data_num) != 3 ){
							count ++;
						}
						
						send_package( &ppl->pool[pos], pos, \
						buffer_per_size*send_pos, count%qpmgt->ctrl_num+qpmgt->data_num);
						
						count ++;
						
						//fprintf(stderr, "submit package %p id %d\n", &ppl->pool[pos], pos);
						fprintf(stderr, "submit package id %04d send id %04d\n", pos, send_pos);
						
					}
					else pthread_mutex_unlock(&sbf->sbf_mutex);
					
					cq_write += elapse_sec()-tmp_time;
				}
				
				if( wc->opcode == IBV_WC_SEND ){
					double tmp_time = elapse_sec();
					
					struct package_active *now;
					now = ( struct package_active * )wc->wr_id;
					if( wc->status != IBV_WC_SUCCESS ){
						if( now->resend_count >= resend_limit ){
							fprintf(stderr, "package %d wrong after resend %d times\n", \
							((ull)now-(ull)ppl->pool)/sizeof(struct package_active), now->resend_count);
							for( int i = 0; i < now->number; i ++ ){
								for( int j = 0; j < now->scatter[i]->number; j ++ ){
									now->scatter[i]->task[j]->state = -1;
									fprintf(stderr, "request %llu failure\n", \
									now->scatter[i]->task[j]->request->private);
								}
							}
							TEST_NZ(clean_send_buffer(now));
							TEST_NZ(clean_package(now));
						}
						else{
							now->resend_count ++;
							int pos = ((ull)now-(ull)ppl->pool)/sizeof( struct package_active );
							while( re_qp_query(count%qpmgt->ctrl_num+qpmgt->data_num) != 3 ){
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
					TEST_NZ(clean_send_buffer(now));
					
					cq_send += elapse_sec()-tmp_time;
				}
				
				if( wc->opcode == IBV_WC_RECV ){
					double tmp_time = elapse_sec();
					
					if( qp_query(wc->wr_id/recv_imm_data_num+qpmgt->data_num) == 3 )
						post_recv( wc->wr_id/recv_imm_data_num+qpmgt->data_num,\
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
					
					TEST_NZ(clean_package(now));
					
					cq_recv += elapse_sec()-tmp_time;
				}
			}
			if( tot >= 150 ){ tot = 0; break; }
		}
	}
}

void huawei_asyn_send( struct request_active *rq )
{
	pthread_mutex_lock(&rbf->rbf_mutex);
	while( rbf->count == request_buffer_size ){	
		pthread_cond_wait( &thpl->cond1, &rbf->rbf_mutex );
	}
	rbf->buffer[rbf->front++] = rq;
	if( rbf->front >= request_buffer_size ) rbf->front -= request_buffer_size;
	rbf->count ++;

	pthread_mutex_unlock(&rbf->rbf_mutex);
	
	/* signal working thread */
	pthread_cond_signal( &thpl->cond0 );

	return ;
}

void *full_time_send()
{
	struct scatter_active *buffer[64];
	int num, cnt = 0;
	uint sleep_time = full_time_interval;
	fprintf(stderr, "full_time_send thread ready\n");
	pthread_mutex_lock(&sbf->signal_mutex);
	pthread_cond_wait(&sbf->signal_cond, &sbf->signal_mutex);
	pthread_mutex_unlock(&sbf->signal_mutex);
	while(!sbf->shutdown){
		usleep(sleep_time);
		double tmp = elapse_sec()-sbf->start;
		//printf("start %.5lf now %.5lf tmp %.5lf\n", sbf->start, sbf->start+tmp, tmp);
		if( tmp < full_time_interval ){
			sleep_time = full_time_interval-(uint)tmp;
			continue;
		}
		pthread_mutex_lock(&sbf->sbf_mutex);
		if( sbf->cnt != 0 ){
			//printf("full time send start\n");
			for( int i = 0; i < sbf->cnt; i ++ )
				buffer[i] = sbf->buffer[i];
			num = sbf->cnt;
			sbf->cnt = 0;
			pthread_mutex_unlock(&sbf->sbf_mutex);
			/* only need mutex above and ?_pos */
			
			//pthread_mutex_lock(&ppl->package_mutex);
			int pos = query_bit_free( ppl->bit, 0, package_pool_size );
			if( pos==-1 ){
				fprintf(stderr, "no more space while finding package_pool\n");
				exit(1);
			}
			//pthread_mutex_unlock(&ppl->package_mutex);
			
			ppl->pool[pos].number = num;
			ppl->pool[pos].resend_count = 1;
			for( int i = 0; i < ppl->pool[pos].number; i ++ ){
				ppl->pool[pos].scatter[i] = buffer[i];
			}
			
			/* initialize send ack buffer */
			pthread_mutex_lock(&memgt->send_mutex);
			int send_pos = query_bit_free( memgt->send_bit, 0, \
			BUFFER_SIZE/buffer_per_size ); 
			if( send_pos==-1 ){
				fprintf(stderr, "no more space while finding send_buffer\n");
				exit(1);
			}
			pthread_mutex_unlock(&memgt->send_mutex);
			
			ppl->pool[pos].send_buffer_id = send_pos;
			
			int count = rand();
			while( qp_query(count%qpmgt->ctrl_num+qpmgt->data_num) != 3 ){
				count ++;
			}
			
			send_package( &ppl->pool[pos], pos, \
			buffer_per_size*send_pos, count%qpmgt->ctrl_num+qpmgt->data_num);

			cnt ++;
			fprintf(stderr, "full time submit package id %04d send id %04d\n", pos, send_pos);
		}
		else pthread_mutex_unlock(&sbf->sbf_mutex);
		sleep_time = full_time_interval;
		sbf->start = elapse_sec();
	}
	fprintf(stderr, "full time thread end count %d\n", cnt);
}
