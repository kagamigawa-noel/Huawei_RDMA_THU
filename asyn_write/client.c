#include "common.h"

struct request_buffer
{
	struct request_active **buffer;
	int count, front, tail; 
	pthread_mutex_t rbf_mutex;
	pthread_spinlock_t spin;
};

struct scatter_buffer
{
	struct scatter_active **buffer;
	int cnt, shutdown;
	double start;
	pthread_t signal_id;
	pthread_mutex_t sbf_mutex, signal_mutex;
	pthread_cond_t signal_cond;
	pthread_spinlock_t spin;
};

struct task_pool
{
	int size;
	struct task_active *pool;
	struct bitmap *btmp;
};

struct scatter_pool
{
	int size;
	struct bitmap *btmp;
	struct scatter_active *pool;
};

struct package_pool
{
	int size;
	struct package_active *pool;
	struct bitmap *btmp;
};

struct thread_pool
{
	int number, tmp[5], shutdown;
	pthread_cond_t cond0, cond1;
	pthread_t completion_id, pthread_id[5];
};

struct rdma_addrinfo *addr;
struct request_buffer *rbf;
struct scatter_buffer *sbf;
struct task_pool *tpl[4];
struct scatter_pool *spl[4];
struct package_pool *ppl;
struct thread_pool *thpl;
int send_package_count, write_count, request_count;
struct timeval test_start;
extern double get_working, do_working, \
cq_send, cq_recv, cq_write, cq_waiting, cq_poll, q_task, other,\
send_package_time, end_time, base, working_write, q_qp,\
init_remote, init_scatter, q_scatter, one_rq_end, one_rq_start,\
sum_tran, sbf_time, callback_time, get_request;
extern int dt[300005], d_count, send_new_id, rq_sub, mx;

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
	/* 回收空间 */
	/* clean task pool*/
	num = 0;
	for( int j = 0; j < now->number; j ++ ){
		data[num++] = ( (ull)now->task[j] - (ull)tpl[now->belong]->pool )/sizeof(struct task_active);
	}
	reback = update_bitmap( tpl[now->belong]->btmp, data, num );
	if( reback != 0 ){
		fprintf(stderr, "update task_pool failure %d\n", reback);
		exit(1);
	}
	//fprintf(stderr, "clean task pool %d\n", reback);
	
	/* clean peer bit */
	data[0] = ((ull)now->remote_sge.address-(ull)memgt->peer_mr.addr)\
	/(scatter_size*request_size)%memgt->peer[now->belong]->size;
	
	reback = update_bitmap( memgt->peer[now->belong], data, 1 );
	if( reback != 0 ){
		fprintf(stderr, "update remote_region failure %d\n", reback);
		exit(1);
	}
	//fprintf(stderr, "clean peer bit %d\n", reback);
	
	/* clean scatter pool */
	data[0] = ( (ull)now-(ull)spl[now->belong]->pool )/sizeof( struct scatter_active );
	reback = update_bitmap( spl[now->belong]->btmp, data, 1 );
	if( reback != 0 ){
		fprintf(stderr, "update scatter_pool failure %d\n", reback);
		exit(1);
	}
	//fprintf(stderr, "clean scatter pool %d\n", reback);
	
	return 0;
}

int clean_send_buffer( struct package_active *now )
{
	int data[10];
	data[0] = now->send_buffer_id;
	int reback = update_bitmap(memgt->send, data, 1);
	if( reback ){
		printf("update send_buffer failure %d\n", data[0]);
		exit(1);
	}
	//fprintf(stderr, "recycle send_buffer %d\n", data[0]);
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
	reback = update_bitmap(ppl->btmp, data, 1);
	if( reback ){
		printf("update packaget failure %d\n", data[0]);
		exit(1);
	}
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
			get_wc( &wc );
			
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
	get_wc( &wc );
	
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
	pthread_spin_init(&rbf->spin, NULL);
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
#ifndef _TEST_SYN	
#ifdef __TIMEOUT_SEND	
	pthread_create( &sbf->signal_id, NULL, full_time_send, NULL );
#endif
#endif	
	fprintf(stderr, "initialize scatter_buffer end\n");
	
	/*initialize pool*/
	fprintf(stderr, "initialize pool begin\n");
	for( int i = 0; i < thread_number; i ++ ){
		tpl[i] = ( struct task_pool * ) malloc( sizeof( struct task_pool ) );
		spl[i] = ( struct scatter_pool * ) malloc( sizeof( struct scatter_pool ) );
		tpl[i]->pool = ( struct task_active * )malloc( sizeof(struct task_active)*task_pool_size );
		spl[i]->pool = ( struct scatter_active * )malloc( sizeof(struct scatter_active)*scatter_pool_size );
		init_bitmap(&tpl[i]->btmp, task_pool_size);
		init_bitmap(&spl[i]->btmp, scatter_pool_size);
	}
	
	ppl = ( struct package_pool * ) malloc( sizeof( struct package_pool ) );
	ppl->pool = ( struct package_active * )malloc( sizeof(struct package_active)*package_pool_size );
	init_bitmap(&ppl->btmp, package_pool_size);

	for( int i = 0; i < thread_number; i ++ ){
		init_bitmap(&memgt->peer[i], RDMA_BUFFER_SIZE/(scatter_size*request_size)/thread_number);
	}
	init_bitmap(&memgt->send, BUFFER_SIZE/buffer_per_size);
	fprintf(stderr, "initialize pool end\n");
	
	/*create pthread pool*/
	fprintf(stderr, "create pthread pool begin\n");
	thpl = (struct thread_pool *)malloc(sizeof(struct thread_pool));
#ifndef _TEST_SYN	
	pthread_create( &thpl->completion_id, NULL, completion_active, NULL );
#endif
	
	pthread_cond_init(&thpl->cond0, NULL);
	pthread_cond_init(&thpl->cond1, NULL);
	
	thpl->number = thread_number;
	thpl->shutdown = 0;
	
	for( int i = 0; i < thread_number; i ++ ){
		thpl->tmp[i] = i;
#ifndef _TEST_SYN			
		pthread_create( &thpl->pthread_id[i], NULL, working_thread, &thpl->tmp[i] );
#endif
	}
	fprintf(stderr, "create pthread pool end\n");
}

void finalize_active()
{
	/* destroy pthread pool */
	if( !thpl->shutdown ){
		thpl->shutdown = 1;
#ifdef __MUTEX
		pthread_cond_broadcast(&thpl->cond0);
#endif
		for( int i = 0; i < thpl->number; i ++ ){
#ifndef __MUTEX
			//TEST_NZ(pthread_cancel(thpl->pthread_id[i]));
#else
			TEST_NZ(pthread_join(thpl->pthread_id[i], NULL));
#endif
		}
		TEST_NZ(pthread_cancel(thpl->completion_id));
		TEST_NZ(pthread_join(thpl->completion_id, NULL));
		
		TEST_NZ(pthread_cond_destroy(&thpl->cond0));
		TEST_NZ(pthread_cond_destroy(&thpl->cond1));
		
		free(thpl); thpl = NULL;
	}
	fprintf(stderr, "destroy pthread pool success\n");
	
	/* destroy request buffer */
	TEST_NZ(pthread_mutex_destroy(&rbf->rbf_mutex));
	TEST_NZ(pthread_spin_destroy(&rbf->spin));
	free(rbf->buffer); rbf->buffer = NULL;
	free(rbf); rbf = NULL;
	fprintf(stderr, "destroy request buffer success\n");
	
	/* destroy scatter buffer */
	sbf->shutdown = 1;
	usleep(full_time_interval);
	TEST_NZ(pthread_mutex_destroy(&sbf->sbf_mutex));
	TEST_NZ(pthread_mutex_destroy(&sbf->signal_mutex));
	TEST_NZ(pthread_cond_destroy(&sbf->signal_cond));
#ifdef __TIMEOUT_SEND		
	TEST_NZ(pthread_join(sbf->signal_id, NULL));
#endif
	free(sbf->buffer); sbf->buffer = NULL;
	free(sbf); sbf = NULL;	
	fprintf(stderr, "destroy scatter buffer success\n");
	
	/* destroy task pool */
	for( int i = 0; i < thread_number; i ++ ){
		printf("task pool %#d remain %d\n", i, query_bitmap_free(tpl[i]->btmp));
		final_bitmap(tpl[i]->btmp);
		free(tpl[i]->pool); tpl[i]->pool = NULL;
		free(tpl[i]); tpl[i] = NULL;
	}
	fprintf(stderr, "destroy task pool success\n");
	
	/* destroy scatter pool */
	for( int i = 0; i < thread_number; i ++ ){
		printf("scatter pool %#d remain %d\n", i, query_bitmap_free(spl[i]->btmp));
		final_bitmap(spl[i]->btmp);
		free(spl[i]->pool); spl[i]->pool = NULL;
		free(spl[i]); spl[i] = NULL;
	}
	fprintf(stderr, "destroy scatter pool success\n");
	
	/* destroy package pool */
	printf("package pool remain %d\n", query_bitmap_free(ppl->btmp));
	final_bitmap(ppl->btmp);
	free(ppl->pool); ppl->pool = NULL;
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
	int thread_id = (*(int *)arg), i, j, cnt = 0, t_pos, s_pos, m_pos, qp_num;
	uint count = 0;
	qp_num = qpmgt->data_num/thread_number;
	struct request_active *now, *request_buffer[10];
	struct task_active *task_buffer[10];
	fprintf(stderr, "working thread #%d ready\n", thread_id);
	//sleep(5);
	while(1){
		double tmp_time = elapse_sec();
		for( i = 0; i < qp_num; i ++ ){
			//mx = max( qpmgt->qp_count[i+thread_id*qp_num], mx );
			if( qpmgt->qp_count[i+thread_id*qp_num] < qp_size_limit ) break;
		}
		if( i == qp_num ){
			//usleep(work_timeout);
			get_working += elapse_sec()-tmp_time;
			continue;
		}
		tmp_time = elapse_sec();
#ifdef __MUTEX
		pthread_mutex_lock(&rbf->rbf_mutex);
		DEBUG("working thread #%d lock\n", thread_id);
		while( rbf->count <= 0 && !thpl->shutdown ){
			pthread_cond_wait( &thpl->cond0, &rbf->rbf_mutex );
		}
#else
		while(1){
			pthread_spin_lock(&rbf->spin);
			if( rbf->count <= 0 && !thpl->shutdown ){
				pthread_spin_unlock(&rbf->spin);
				continue;
			}
			else break;
		}
#endif
		if( thpl->shutdown ){
#ifdef __MUTEX
			pthread_mutex_unlock(&rbf->rbf_mutex);
#else
			pthread_spin_unlock(&rbf->spin);
#endif
			pthread_exit(0);
		}
		while( rbf->count > 0 && cnt < scatter_size ){
			rbf->count --;				
			request_buffer[cnt++] = rbf->buffer[rbf->tail++];
			if( rbf->tail >= request_buffer_size ) rbf->tail -= request_buffer_size;
		}
#ifdef __MUTEX
		pthread_mutex_unlock(&rbf->rbf_mutex);
#else
		pthread_spin_unlock(&rbf->spin);
#endif
		/* signal api */
#ifdef __MUTEX		
		pthread_cond_signal( &thpl->cond1 );
#endif		
		
		get_working += elapse_sec()-tmp_time;
		tmp_time = elapse_sec();
		
		for( i = 0; i < cnt; i ++ ){
			request_buffer[i]->get = elapse_sec();
		}
		
		for( i = 0; i < cnt; i ++ ){
			if( tpl[thread_id]->btmp->size != task_pool_size ){
				printf("now size %d origin %d\n", tpl[thread_id]->btmp->size, task_pool_size );
				tpl[thread_id]->btmp->size = task_pool_size;
			}
			t_pos = query_bitmap( tpl[thread_id]->btmp );
			if( t_pos == -1 ){
				printf("task_pool %d\n", thread_id);
				exit(1);
			}
			
			/* initialize task_active */
			tpl[thread_id]->pool[t_pos].request = request_buffer[i];
			tpl[thread_id]->pool[t_pos].state = 0;
			DEBUG("working thread #%d r_id %llu task %d\n",\
			thread_id, tpl[thread_id]->pool[t_pos].request->private, t_pos);
			
			task_buffer[i] = &tpl[thread_id]->pool[t_pos];
		}
		
		q_task += elapse_sec()-tmp_time;
		tmp_time = elapse_sec();
		
		if( spl[thread_id]->btmp->size != scatter_pool_size ){
			printf("now size %d origin %d\n", spl[thread_id]->btmp->size, scatter_pool_size );
			spl[thread_id]->btmp->size = scatter_pool_size;
		}
		s_pos = query_bitmap( spl[thread_id]->btmp );
		if( s_pos==-1 ){
			fprintf(stderr, "no more space while finding scatter_pool\n");
			exit(1);
		}
		
		q_scatter += elapse_sec()-tmp_time;
		tmp_time = elapse_sec();
		
		spl[thread_id]->pool[s_pos].number = cnt;
		spl[thread_id]->pool[s_pos].belong = thread_id;
		for( j = 0; j < cnt; j ++ ){
			spl[thread_id]->pool[s_pos].task[j] = task_buffer[j];
			task_buffer[j]->state = 1;
			// can add sth to calculate memory size
		}
		
		spl[thread_id]->pool[s_pos].remote_sge.next = NULL;
		
		m_pos = query_bitmap( memgt->peer[thread_id] );// block waiting time
		
		//四个bitmap管理一个区域，加偏移
		spl[thread_id]->pool[s_pos].remote_sge.address = \
		memgt->peer_mr.addr+(m_pos+memgt->peer[thread_id]->size*thread_id)\
		*( request_size*scatter_size );
		spl[thread_id]->pool[s_pos].remote_sge.length = request_size*cnt;
		void *start = spl[thread_id]->pool[s_pos].remote_sge.address;
		for( j = 0; j < cnt; j ++ ){
			spl[thread_id]->pool[s_pos].task[j]->remote_sge.address = start;
			spl[thread_id]->pool[s_pos].task[j]->remote_sge.length = \
			spl[thread_id]->pool[s_pos].task[j]->request->sl->length;
			start += spl[thread_id]->pool[s_pos].task[j]->remote_sge.length;
		}
		
		init_remote += elapse_sec()-tmp_time;
		tmp_time = elapse_sec();
		
		while( qp_query(thread_id*qp_num+count%qp_num) != 3 ){
			count ++;
		}
		int tmp_qp_id = thread_id*qp_num+count%qp_num;
		count ++;
		spl[thread_id]->pool[s_pos].qp_id = tmp_qp_id;
		spl[thread_id]->pool[s_pos].resend_count = 1;
		
		post_rdma_write( tmp_qp_id, &spl[thread_id]->pool[s_pos] );

		DEBUG("working thread #%d submit scatter %04d qp %02d remote %04d\n", \
		thread_id, s_pos, tmp_qp_id, m_pos);
		
		//usleep(work_timeout);
		cnt = 0;
	}
}

void *completion_active()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	int i, j, k, num;
	uint count = 0;
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
		
		int tot = 0;
		while(1){
			double tmp_time = elapse_sec();
			num = ibv_poll_cq(cq, 100, wc_array);
			
			if( num <= 0 ) break;
			tot += num;
			//fprintf(stderr, "%04d CQE get!!!\n", num);
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
					for( j = 0; j < thread_number; j ++ ){
						if( ((ull)now-(ull)spl[j]->pool)/sizeof(struct scatter_active) >= 0 &&
						((ull)now-(ull)spl[j]->pool)/sizeof(struct scatter_active) < scatter_pool_size )
						break;
						else continue;
					}
					if( j == thread_number ){
						printf("wrong back scatter %d\n", ((ull)now-(ull)spl[j]->pool)/sizeof(struct scatter_active));
						exit(1);
					}
					if( wc->status != IBV_WC_SUCCESS ){
						//printf("id: %lld qp: %d\n", ((ull)now-(ull)spl->pool)/sizeof(struct scatter_active), now->qp_id);
						if( now->resend_count >= resend_limit ){
							fprintf(stderr, "scatter %d wrong after resend %d times\n", \
							((ull)now-(ull)spl[now->belong]->pool)/sizeof(struct scatter_active), now->resend_count);
							// can add sth to avoid the death of this scatter
							for( int i = 0; i < now->number; i ++ ){
								now->task[i]->state = -1;
								fprintf(stderr, "request %llu failure\n", \
								now->task[i]->request->private);
							}
							clean_scatter(now);
						}
						else{
							qpmgt->qp_count[now->qp_id] --;
							now->resend_count ++;
							while( re_qp_query(count%qpmgt->data_num) != 3 ){
								count ++;
							}
							now->qp_id = count%qpmgt->data_num;
							count ++;
							
							post_rdma_write( now->qp_id, now );
							fprintf(stderr, "completion thread resubmit scatter %d #%d wa %d\n", \
							((ull)now-(ull)spl[now->belong]->pool)/sizeof(struct scatter_active), now->resend_count, wc->status);
						}
						continue;
					}
					write_count ++;
					request_count += now->number;
					for( int i = 0; i < now->number; i ++ ){
						now->task[i]->request->back = elapse_sec();
					}
					double tmp_time = elapse_sec();
					pthread_mutex_lock(&sbf->sbf_mutex);
					sbf->buffer[sbf->cnt++] = now;
					pthread_mutex_unlock(&sbf->sbf_mutex);

					DEBUG("get CQE scatter %04lld\n", ((ull)now-(ull)spl[now->belong]->pool)/sizeof(struct scatter_active));

					for( i = 0; i < now->number; i ++ ){
						if( now->task[i]->state == 2 ){
							printf("wrong back state 2 %d\n", now->task[i]->request->private);
							break;
						}
						now->task[i]->state = 2;
						/*operate request callback*/
						now->task[i]->request->callback(now->task[i]->request);
						dt[d_count++] = now->task[i]->request->private;
					}
					if( i != now->number ) continue;

					qpmgt->qp_count[now->qp_id] --;
					
					pthread_mutex_lock(&sbf->sbf_mutex);
					if( sbf->cnt == package_size ){
						sbf->start = elapse_sec();
						int num = sbf->cnt;
						sbf->cnt = 0;
						for( i = 0; i < num; i ++ ) buffer[i] = sbf->buffer[i];
						pthread_mutex_unlock(&sbf->sbf_mutex);
						
						/* initialize package_active */
						int pos = query_bitmap( ppl->btmp );
						if( pos == -1 ){
							printf("package_pool\n");
							exit(1);
						}
						
						//fprintf(stderr, "pos %04d\n", pos);
						ppl->pool[pos].number = num;
						ppl->pool[pos].resend_count = 1;
						for( i = 0; i < ppl->pool[pos].number; i ++ ){
							ppl->pool[pos].scatter[i] = buffer[i];
						}
						
						/* initialize send ack buffer */
						int send_pos = query_bitmap( memgt->send ); 
						if( send_pos == -1 ){
							printf("send_buffer\n");
							exit(1);
						}
						
						ppl->pool[pos].send_buffer_id = send_pos;
						
						while( qp_query(count%qpmgt->ctrl_num+qpmgt->data_num) != 3 ){
							count ++;
						}
						
						ppl->pool[pos].qp_id = count%qpmgt->ctrl_num+qpmgt->data_num;
						qpmgt->qp_count[ppl->pool[pos].qp_id] ++;
						double tmp_time = elapse_sec();
						send_package( &ppl->pool[pos], pos, \
						buffer_per_size*send_pos, count%qpmgt->ctrl_num+qpmgt->data_num);
						send_package_time += elapse_sec()-tmp_time;
						
						count ++;
						
						DEBUG("submit package id %04d send id %04d\n", pos, send_pos);
						
					}
					else pthread_mutex_unlock(&sbf->sbf_mutex);
					
					cq_write += elapse_sec()-tmp_time;
				}
				
				if( wc->opcode == IBV_WC_SEND ){
					double tmp_time = elapse_sec();
					
					struct package_active *now;
					now = ( struct package_active * )wc->wr_id;
					if( ((ull)now-(ull)ppl->pool)/sizeof(struct package_active) < 0 ||
					((ull)now-(ull)ppl->pool)/sizeof(struct package_active) >= package_pool_size ){
						printf("wrong back package %d\n", ((ull)now-(ull)ppl->pool)/sizeof(struct package_active));
						exit(1);
					}
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
					DEBUG("send package %04d success\n", \
					((ull)now-(ull)ppl->pool)/sizeof( struct package_active ));
					send_package_count ++;
					qpmgt->qp_count[now->qp_id] --;
					
					for( int i = 0; i < now->number; i ++ ){
						for( int j = 0; j < now->scatter[i]->number; j ++ ){
							if( now->scatter[i]->task[j]->state == 3 ){
								//printf("wrong back status 3 %d\n", now->scatter[i]->task[j]->request->private);
							}
							now->scatter[i]->task[j]->state = 3;
						}
					}
					
					/* clean send buffer */
					TEST_NZ(clean_send_buffer(now));
					
					cq_send += elapse_sec()-tmp_time;
				}
				
				if( wc->opcode == IBV_WC_RECV ){
					double tmp_time = elapse_sec();
					
					//if( qp_query(wc->wr_id/recv_imm_data_num+qpmgt->data_num) == 3 )
						post_recv( wc->wr_id/recv_imm_data_num+qpmgt->data_num,\
					 wc->wr_id, 0 );
					//else continue;
					
					struct package_active *now;
					if( wc->imm_data < 0 || wc->imm_data >= package_pool_size ){
						printf("wrong recv imm_data %d\n", wc->imm_data);
						exit(1);
					}
					now = &ppl->pool[wc->imm_data];
					
					DEBUG("get CQE package id %d\n", wc->imm_data);
					
					for( int i = 0; i < now->number; i ++ ){
						for( int j = 0; j < now->scatter[i]->number; j ++ ){
							now->scatter[i]->task[j]->state = 4;
						}
					}
					
					TEST_NZ(clean_package(now));
					
					end_time = elapse_sec()-base;
				}
			}
			//if( tot >= 150 ){ tot = 0; break; }
		}
	}
}

void huawei_asyn_send( struct request_active *rq )
{
#ifdef __MUTEX
	pthread_mutex_lock(&rbf->rbf_mutex);
	while( rbf->count == request_buffer_size ){	
		pthread_cond_wait( &thpl->cond1, &rbf->rbf_mutex );
	}
#else
	while(1){
		pthread_spin_lock(&rbf->spin);
		if( rbf->count == request_buffer_size ){
			pthread_spin_unlock(&rbf->spin);
			continue;
		}
		else break;
	}
#endif

	rbf->buffer[rbf->front++] = rq;
	if( rbf->front >= request_buffer_size ) rbf->front -= request_buffer_size;
	rbf->count ++;

#ifdef __MUTEX
	pthread_mutex_unlock(&rbf->rbf_mutex);
	/* signal working thread */
	pthread_cond_signal( &thpl->cond0 );
#else
	pthread_spin_unlock(&rbf->spin);
#endif

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
			
			int pos = query_bitmap( ppl->btmp );
			if( pos == -1 ){
				printf("package_pool\n");
				exit(1);
			}
			
			ppl->pool[pos].number = num;
			ppl->pool[pos].resend_count = 1;
			for( int i = 0; i < ppl->pool[pos].number; i ++ ){
				ppl->pool[pos].scatter[i] = buffer[i];
			}
			
			/* initialize send ack buffer */
			int send_pos = query_bitmap( memgt->send ); 
			if( send_pos == -1 ){
				printf("send_buffer\n");
				exit(1);
			}
			
			ppl->pool[pos].send_buffer_id = send_pos;
			
			int count = rand();
			while( qp_query(count%qpmgt->ctrl_num+qpmgt->data_num) != 3 ){
				count ++;
			}
			
			send_package( &ppl->pool[pos], pos, \
			buffer_per_size*send_pos, count%qpmgt->ctrl_num+qpmgt->data_num);

			cnt ++;
			fprintf(stderr, "full time submit package id %04d send id %04d\n", pos, send_pos);
			for( int i = 0; i < ppl->pool[pos].number; i ++ ){
				for( int j = 0; j < ppl->pool[pos].scatter[i]->number; j ++ )
				printf("%llu ", ppl->pool[pos].scatter[i]->task[j]->request->private);
			}
			printf("\n");
		}
		else pthread_mutex_unlock(&sbf->sbf_mutex);
		sleep_time = full_time_interval;
		sbf->start = elapse_sec();
	}
	fprintf(stderr, "full time thread end count %d\n", cnt);
}
