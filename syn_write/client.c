#include "common.h"

struct request_buffer
{
	struct request_active **buffer;
	int count, front, tail; 
	pthread_mutex_t rbf_mutex;
	pthread_spinlock_t spin;
};

struct task_pool
{
	struct task_active *pool;
	struct bitmap *btmp;
};

struct thread_pool
{
	int number, tmp[5], shutdown;
	pthread_mutex_t mutex0, mutex1;
	pthread_cond_t cond0, cond1;
	pthread_t completion_id, pthread_id[5];
};

struct rdma_addrinfo *addr;
struct request_buffer *rd_rbf, *wt_rbf;
struct task_pool *rd_tpl[4], *wt_tpl[4];
struct thread_pool *rd_thpl, *wt_thpl;
int write_count, request_count, back_count;
struct timeval test_start;
extern double end_time;

void initialize_active( void *address, int length, char *ip_address );
void finalize_active();
int on_event(struct rdma_cm_event *event, int tid);
void *wt_working_thread(void *arg);
void *rd_working_thread(void *arg);
void *wt_completion_active();
void *rd_completion_active();
void huawei_send( struct request_active *rq );
int clean_task( struct task_active *now, enum type tp );
enum type evaluation();

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

int clean_task( struct task_active *now, enum type tp )
{
	struct memory_management *memgt;
	struct task_pool *tpl;
	if( tp == READ ){ memgt = rd_memgt; tpl = rd_tpl[now->belong]; }
	else{ memgt = wt_memgt; tpl = wt_tpl[now->belong]; }
	int data[20], num = 0, reback = 0;
	/* 回收空间 + 修改peer_bit */
	/* clean task pool*/
	data[0] = ( (ull)now-(ull)tpl->pool )/sizeof( struct task_active );
	update_bitmap( tpl->btmp, data, 1 );
	//fprintf(stderr, "clean task pool %d\n", reback);
	
	if( tp == WRITE ){
		/* clean peer bit */
		data[0] = ((ull)now->remote_sge.address-(ull)memgt->peer_mr.addr)\
		/(request_size+metedata_size)%memgt->peer[now->belong]->size;
		update_bitmap( memgt->peer[now->belong], data, 1 );
		//fprintf(stderr, "clean peer bit %d\n", reback);
	}
	else{
		data[0] = ((ull)now->remote_sge.address-(ull)memgt->peer_mr.addr)\
		/(request_size)%memgt->peer[now->belong]->size;
		update_bitmap( memgt->peer[now->belong], data, 1 );
	}
	//读写不一样，读是用send传递元数据，写是用write传递
	return 0;
}

void initialize_active( void *address, int length, char *ip_address )
{
	end = 0;
	s_ctx = ( struct connection * )malloc( sizeof( struct connection ) );
	rd_memgt = ( struct memory_management * ) malloc( sizeof( struct memory_management ) );
	wt_memgt = ( struct memory_management * ) malloc( sizeof( struct memory_management ) );
	rd_qpmgt = ( struct qp_management * ) malloc( sizeof( struct qp_management ) );
	wt_qpmgt = ( struct qp_management * ) malloc( sizeof( struct qp_management ) );
	rd_memgt->application.next = NULL;
	rd_memgt->application.address = address;
	rd_memgt->application.length = length;
	wt_memgt->application.next = NULL;
	wt_memgt->application.address = address;
	wt_memgt->application.length = length;
	
	struct ibv_wc wc;
	for( int i = 0; i < connect_number*2; i ++ ){
		if( i == 0 ){
			char port[20];
			sprintf(port, "%d\0", bind_port);
			TEST_NZ(rdma_getaddrinfo(ip_address, port, NULL, &addr));
		}
		else{
			post_recv( 0, i, 0, 0, READ );
			TEST_NZ( get_wc( &wc, READ ) );
			
			char port[20];
			fprintf(stderr, "port: %d\n", wc.imm_data);
			sprintf(port, "%d\0", wc.imm_data);
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
		//fprintf(stderr, "build connect succeed %d\n", i);
	}
	
	post_recv( 0, 0, 0, sizeof(struct ibv_mr), WRITE );
	TEST_NZ( get_wc( &wc, WRITE ) );
	
	memcpy( &wt_memgt->peer_mr, wt_memgt->recv_buffer, sizeof(struct ibv_mr) );
	printf("write add: %p length: %d\n", wt_memgt->peer_mr.addr,
	wt_memgt->peer_mr.length);
	
	memcpy( rd_memgt->send_buffer, rd_memgt->rdma_send_mr, sizeof(struct ibv_mr) );
	printf("read add: %p length: %d\n", rd_memgt->rdma_send_mr->addr,
	rd_memgt->rdma_send_mr->length);
	
	post_send( 0, 0, 0, sizeof(struct ibv_mr), 0, READ );
	TEST_NZ( get_wc( &wc, READ ) );
	
	for( int i = rd_qpmgt->data_num; i < rd_qpmgt->data_num+rd_qpmgt->ctrl_num; i ++ ){
		for( int j = 0; j < recv_imm_data_num; j ++ )
			post_recv( i, (i-rd_qpmgt->data_num)*recv_imm_data_num+j, 0, 0, READ );
	}
	
	for( int i = wt_qpmgt->data_num; i < wt_qpmgt->data_num+wt_qpmgt->ctrl_num; i ++ ){
		for( int j = 0; j < recv_imm_data_num; j ++ )
			post_recv( i, (i-wt_qpmgt->data_num)*recv_imm_data_num+j, 0, 0, WRITE );
	}
	
	/* test sth */
	write_count = 0;
	request_count = 0;
	back_count = 0;
	
	/*initialize request_buffer*/
	fprintf(stderr, "initialize request_buffer begin\n");
	rd_rbf = ( struct request_buffer * )malloc( sizeof( struct request_buffer ) );
	pthread_mutex_init(&rd_rbf->rbf_mutex, NULL);
	pthread_spin_init(&rd_rbf->spin, NULL);
	rd_rbf->count = 0;
	rd_rbf->front = 0;
	rd_rbf->tail = 0;
	rd_rbf->buffer = ( struct request_active ** )malloc(sizeof(struct request_active *)*request_buffer_size);
	
	wt_rbf = ( struct request_buffer * )malloc( sizeof( struct request_buffer ) );
	pthread_mutex_init(&wt_rbf->rbf_mutex, NULL);
	pthread_spin_init(&wt_rbf->spin, NULL);
	wt_rbf->count = 0;
	wt_rbf->front = 0;
	wt_rbf->tail = 0;
	wt_rbf->buffer = ( struct request_active ** )malloc(sizeof(struct request_active *)*request_buffer_size);
	fprintf(stderr, "initialize request_buffer end\n");
	
	/*initialize pool*/
	fprintf(stderr, "initialize pool begin\n");
	for( int i = 0; i < thread_number; i ++ ){
		rd_tpl[i] = ( struct task_pool * ) malloc( sizeof( struct task_pool ) );
		rd_tpl[i]->pool = ( struct task_active * )malloc( sizeof(struct task_active)*task_pool_size );
		init_bitmap(&rd_tpl[i]->btmp, task_pool_size);
		init_bitmap(&rd_memgt->peer[i], RDMA_BUFFER_SIZE/(request_size*thread_number));
		init_bitmap(&rd_memgt->send[i], BUFFER_SIZE/(buffer_per_size*thread_number));
	}

	for( int i = 0; i < thread_number; i ++ ){
		wt_tpl[i] = ( struct task_pool * ) malloc( sizeof( struct task_pool ) );
		wt_tpl[i]->pool = ( struct task_active * )malloc( sizeof(struct task_active)*task_pool_size );
		init_bitmap(&wt_tpl[i]->btmp, task_pool_size);
		init_bitmap(&wt_memgt->peer[i], RDMA_BUFFER_SIZE/((request_size+metedata_size)*thread_number));
		init_bitmap(&wt_memgt->send[i], BUFFER_SIZE/(metedata_size*thread_number));
	}
	//buffer_per_size is different from metedata_size
	fprintf(stderr, "initialize pool end\n");
	
	/*create pthread pool*/
	fprintf(stderr, "create pthread pool begin\n");
	rd_thpl = ( struct thread_pool * ) malloc( sizeof( struct thread_pool ) );
	pthread_create( &rd_thpl->completion_id, NULL, rd_completion_active, NULL );
	
	pthread_cond_init(&rd_thpl->cond0, NULL);
	pthread_cond_init(&rd_thpl->cond1, NULL);
	
	rd_thpl->number = thread_number;
	rd_thpl->shutdown = 0;
	
	for( int i = 0; i < thread_number; i ++ ){
		rd_thpl->tmp[i] = i;
		pthread_create( &rd_thpl->pthread_id[i], NULL, rd_working_thread, &rd_thpl->tmp[i] );
	}
	
	wt_thpl = ( struct thread_pool * ) malloc( sizeof( struct thread_pool ) );
	pthread_create( &wt_thpl->completion_id, NULL, wt_completion_active, NULL );
	
	pthread_cond_init(&wt_thpl->cond0, NULL);
	pthread_cond_init(&wt_thpl->cond1, NULL);
	
	wt_thpl->number = thread_number;
	wt_thpl->shutdown = 0;
	
	for( int i = 0; i < thread_number; i ++ ){
		wt_thpl->tmp[i] = i;
		pthread_create( &wt_thpl->pthread_id[i], NULL, wt_working_thread, &wt_thpl->tmp[i] );
	}
	fprintf(stderr, "create pthread pool end\n");
	sleep(2);
}

void finalize_active()
{
	/* destroy pthread pool */
	printf("start finalize\n");
	if( !rd_thpl->shutdown ){
		rd_thpl->shutdown = 1;
		pthread_cond_broadcast(&rd_thpl->cond0);
		for( int i = 0; i < rd_thpl->number; i ++ ){
			TEST_NZ(pthread_join(rd_thpl->pthread_id[i], NULL));
		}
		TEST_NZ(pthread_cancel(rd_thpl->completion_id));
		TEST_NZ(pthread_join(rd_thpl->completion_id, NULL));
		
		TEST_NZ(pthread_cond_destroy(&rd_thpl->cond0));
		TEST_NZ(pthread_cond_destroy(&rd_thpl->cond1));
		
		free(rd_thpl); rd_thpl = NULL;
	}
	
	if( !wt_thpl->shutdown ){
		wt_thpl->shutdown = 1;
		pthread_cond_broadcast(&wt_thpl->cond0);
		for( int i = 0; i < wt_thpl->number; i ++ ){
			TEST_NZ(pthread_join(wt_thpl->pthread_id[i], NULL));
		}
		
		TEST_NZ(pthread_cancel(wt_thpl->completion_id));
		TEST_NZ(pthread_join(wt_thpl->completion_id, NULL));
		
		TEST_NZ(pthread_cond_destroy(&wt_thpl->cond0));
		TEST_NZ(pthread_cond_destroy(&wt_thpl->cond1));
		
		free(wt_thpl); wt_thpl = NULL;
	}
	fprintf(stderr, "destroy pthread pool success\n");
	
	/* destroy request buffer */
	TEST_NZ(pthread_mutex_destroy(&rd_rbf->rbf_mutex));
	TEST_NZ(pthread_spin_destroy(&rd_rbf->spin));
	free(rd_rbf->buffer); rd_rbf->buffer = NULL;
	free(rd_rbf); rd_rbf = NULL;
	
	TEST_NZ(pthread_mutex_destroy(&wt_rbf->rbf_mutex));
	TEST_NZ(pthread_spin_destroy(&wt_rbf->spin));
	free(wt_rbf->buffer); wt_rbf->buffer = NULL;
	free(wt_rbf); wt_rbf = NULL;
	fprintf(stderr, "destroy request buffer success\n");
	
	/* destroy task pool */
	for( int i = 0; i < thread_number; i ++ ){
		final_bitmap(rd_tpl[i]->btmp);
		free(rd_tpl[i]->pool); rd_tpl[i]->pool = NULL;
		free(rd_tpl[i]); rd_tpl[i] = NULL;
	}
	
	for( int i = 0; i < thread_number; i ++ ){
		final_bitmap(wt_tpl[i]->btmp);
		free(wt_tpl[i]->pool); wt_tpl[i]->pool = NULL;
		free(wt_tpl[i]); wt_tpl[i] = NULL;
	}
	
	fprintf(stderr, "destroy task pool success\n");
	
	/* destroy qp management */
	destroy_qp_management(READ);
	destroy_qp_management(WRITE);
	fprintf(stderr, "destroy qp management success\n");
	
	/* destroy memory management */
	destroy_memory_management(end, READ);
	destroy_memory_management(end, WRITE);
	fprintf(stderr, "destroy memory management success\n");
		
	/* destroy connection struct */
	destroy_connection();
	fprintf(stderr, "destroy connection success\n");
	
	fprintf(stderr, "finalize end\n");
}

void *wt_working_thread(void *arg)
{
	int thread_id = (*(int *)arg), i, j, cnt = 0, t_pos, s_pos, m_pos, qp_num;
	uint count = 0;
	ull tmp;
	qp_num = wt_qpmgt->data_num/thread_number;
	struct request_active *now;
	fprintf(stderr, "working thread write #%d ready\n", thread_id);
	while(1){
		for( i = 0; i < qp_num; i ++ ){
			if( query_qp_count( wt_qpmgt, i+thread_id*qp_num ) < qp_size_limit ) break;
		}
		if( i == qp_num ){
			continue;
		}
#ifdef __MUTEX		
		pthread_mutex_lock(&wt_rbf->rbf_mutex);
		//fprintf(stderr, "working thread #%d lock\n", thread_id);
		while( wt_rbf->count <= 0 && !wt_thpl->shutdown ){
			pthread_cond_wait( &wt_thpl->cond0, &wt_rbf->rbf_mutex );
		}
#else
		while(1){
			pthread_spin_lock(&wt_rbf->spin);
			if( wt_rbf->count <= 0 && !wt_thpl->shutdown ){
				pthread_spin_unlock(&wt_rbf->spin);
				continue;
			}
			else break;
		}
#endif	
		if( wt_thpl->shutdown ){
#ifdef __MUTEX
			pthread_mutex_unlock(&wt_rbf->rbf_mutex);
#else
			pthread_spin_unlock(&wt_rbf->spin);
#endif
			pthread_exit(0);
		}
		wt_rbf->count --;				
		now = wt_rbf->buffer[wt_rbf->tail++];
		if( wt_rbf->tail >= request_buffer_size ) wt_rbf->tail -= request_buffer_size;
		//fprintf(stderr, "working thread #%d solve request %p\n", thread_id, now);
		
#ifdef __MUTEX		
		pthread_mutex_unlock(&wt_rbf->rbf_mutex);
#else
		pthread_spin_unlock(&wt_rbf->spin);
#endif
		/* signal api */
#ifdef __MUTEX	
		pthread_cond_signal( &wt_thpl->cond1 );
#endif		
		
		now->get = elapse_sec();
		
		request_count ++;
		t_pos = query_bitmap(wt_tpl[thread_id]->btmp);
			
		/* initialize task_active */
		now->task = &wt_tpl[thread_id]->pool[t_pos];
		wt_tpl[thread_id]->pool[t_pos].request = now;
		wt_tpl[thread_id]->pool[t_pos].state = 0;
		wt_tpl[thread_id]->pool[t_pos].belong = thread_id;
		wt_tpl[thread_id]->pool[t_pos].tp = WRITE;
		//fprintf(stderr, "working thread #%d request %llu task %d\n",\
		thread_id, wt_tpl->pool[t_pos].request->private, t_pos);
		
		m_pos = query_bitmap(wt_memgt->peer[thread_id]);
		m_pos += wt_memgt->peer[thread_id]->size*thread_id;
		//m_pos为绝对位置
		
		wt_tpl[thread_id]->pool[t_pos].remote_sge.address = \
		wt_memgt->peer_mr.addr+m_pos*( request_size+metedata_size );
		wt_tpl[thread_id]->pool[t_pos].remote_sge.length = request_size+metedata_size;
		
		s_pos = query_bitmap(wt_memgt->send[thread_id]);
		s_pos += thread_id*wt_memgt->send[thread_id]->size;
		//send_id为绝对位置
		
		wt_tpl[thread_id]->pool[t_pos].send_id = s_pos;
		tmp = t_pos;
		//这里将t_pos的最高三位压成thread_id
		tmp |= (thread_id<<32-3);
		memcpy( wt_memgt->send_buffer+metedata_size*s_pos, &tmp, sizeof(ull) );
		memcpy( wt_memgt->send_buffer+metedata_size*s_pos+sizeof(ull), &now->private, sizeof(ull) );
		
		while( qp_query(thread_id*qp_num+count%qp_num, WRITE) != 3 ){
			count ++;
		}
		int tmp_qp_id = thread_id*qp_num+count%qp_num;
		count ++;
		wt_tpl[thread_id]->pool[t_pos].qp_id = tmp_qp_id;
		wt_tpl[thread_id]->pool[t_pos].resend_count = 0;
		
		//printf("task %p %d send %d qp %d remote %d\n", &wt_tpl->pool[t_pos], t_pos, s_pos, tmp_qp_id, m_pos);
		
		post_rdma_write( tmp_qp_id, &wt_tpl[thread_id]->pool[t_pos], m_pos );
		//fprintf(stderr, "working thread #%d submit scatter %04d qp: %d %d\n",\
		thread_id, s_pos, tmp_qp_id, *(int *)spl->pool[s_pos].task[i]->request->sl->address);
		// for( int i = 0; i < spl->pool[s_pos].number; i ++ ){
			// fprintf(stderr, " %d", *(int *)spl->pool[s_pos].task[i]->request->sl->address );
		// }
		// fprintf(stderr, "\n");
		// fprintf(stderr, "\n");
		//fprintf(stderr, "working thread #%d submit task %04d qp %02d remote %04d request %llu\n", \
		thread_id, t_pos, tmp_qp_id, m_pos, now->private);
		
		//usleep(work_timeout);
	}
}

void *wt_completion_active()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	int i, j, k, count = 0, num, reback;
	int data[128];
	fprintf(stderr, "completion thread write ready\n");
	while(1){
		TEST_NZ(ibv_get_cq_event(s_ctx->wt_comp_channel, &cq, &ctx));
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
					struct task_active *now;
					now = ( struct task_active * )wc->wr_id;
					if( wc->status != IBV_WC_SUCCESS ){
						if( now->resend_count >= resend_limit ){
							//fprintf(stderr, "task %d wrong after resend %d times\n", \
							((ull)now-(ull)wt_tpl->pool)/sizeof(struct task_active), now->resend_count);
							// can add sth to avoid the death of this scatter
							now->state = -1;
							//fprintf(stderr, "request %llu failure\n", now->request->private);
							clean_task(now, WRITE);
						}
						else{
							now->resend_count ++;
							while( re_qp_query(count%wt_qpmgt->data_num, WRITE) != 3 ){
								count ++;
							}
							now->qp_id = count%wt_qpmgt->data_num;
							count ++;
							
							int m_pos = ((ull)now->remote_sge.address-(ull)wt_memgt->peer_mr.addr)/
							( request_size+metedata_size );
							
							post_rdma_write( now->qp_id, now, m_pos );
							//fprintf(stderr, "completion thread resubmit task %d #%d\n", \
							((ull)now-(ull)wt_tpl->pool)/sizeof(struct task_active), now->resend_count);
						}
						continue;
					}
					write_count ++;
					now->request->back = elapse_sec();
					//DEBUG("rq: %llu back %lf\n", now->request->private, now->request->back);
					//fprintf(stderr, "get CQE task %04lld\n", \
					((ull)now-(ull)wt_tpl->pool)/sizeof(struct task_active));
					now->state = 2;
					
					/* clean send buffer */
					data[0] = now->send_id/wt_memgt->send[now->belong]->size;
					update_bitmap( wt_memgt->send[now->belong], data, 1 );
				}
				
				if( wc->opcode == IBV_WC_RECV ){
					if( qp_query(wc->wr_id/recv_imm_data_num+wt_qpmgt->data_num, WRITE) == 3 )
						post_recv( wc->wr_id/recv_imm_data_num+wt_qpmgt->data_num,\
					 wc->wr_id, 0, 0, WRITE );
					else continue;
					
					struct task_active *now;
					uint belong = (uint)wc->imm_data>>32-3;
					uint id = wc->imm_data-belong;
					now = &wt_tpl[belong]->pool[id];
					DEBUG("belong %u id %u private %llu\n", belong, id, now->request->private);
					//fprintf(stderr, "get CQE task t_pos %d request %d\n", wc->imm_data, now->request->private);
					
					now->state = 3;
					now->request->callback(now->request);
					
					dec_qp_count( wt_qpmgt, now->qp_id );
					TEST_NZ(clean_task(now, WRITE));
					back_count ++;
					
					//end_time = elapse_sec();
				}
			}
			//if( tot >= 150 ){ tot = 0; break; }
		}
	}
}

void *rd_working_thread(void *arg)
{
	int thread_id = (*(int *)arg), i, j, cnt = 0;
	uint t_pos, s_pos, m_pos, qp_num, count = 0;
	qp_num = rd_qpmgt->ctrl_num/thread_number;
	struct request_active *now;
	fprintf(stderr, "working thread read #%d ready\n", thread_id);
	//sleep(5);
	while(1){
		for( i = 0; i < qp_num; i ++ ){
			if( query_qp_count( rd_qpmgt, i+thread_id*qp_num ) < qp_size_limit ) break;
		}
		if( i == qp_num ){
			continue;
		}
		
#ifdef __MUTEX		
		pthread_mutex_lock(&rd_rbf->rbf_mutex);
		//fprintf(stderr, "working thread #%d lock\n", thread_id);
		while( rd_rbf->count <= 0 && !rd_thpl->shutdown ){
			pthread_cond_wait( &rd_thpl->cond0, &rd_rbf->rbf_mutex );
		}
#else
		while(1){
			pthread_spin_lock(&rd_rbf->spin);
			if( rd_rbf->count <= 0 && !rd_thpl->shutdown ){
				pthread_spin_unlock(&rd_rbf->spin);
				continue;
			}
			else break;
		}
#endif	
		if( rd_thpl->shutdown ){
#ifdef __MUTEX
			pthread_mutex_unlock(&rd_rbf->rbf_mutex);
#else
			pthread_spin_unlock(&rd_rbf->spin);
#endif
			pthread_exit(0);
		}
		rd_rbf->count --;				
		now = rd_rbf->buffer[rd_rbf->tail++];
		if( rd_rbf->tail >= request_buffer_size ) rd_rbf->tail -= request_buffer_size;
		//fprintf(stderr, "working thread #%d solve request %p\n", thread_id, now);
		
#ifdef __MUTEX		
		pthread_mutex_unlock(&rd_rbf->rbf_mutex);
#else
		pthread_spin_unlock(&rd_rbf->spin);
#endif
		/* signal api */
#ifdef __MUTEX	
		pthread_cond_signal( &rd_thpl->cond1 );
#endif		
		
		now->get = elapse_sec();
		request_count ++;
		t_pos = query_bitmap( rd_tpl[thread_id]->btmp );
		
		/* initialize task_active */
		now->task = &rd_tpl[thread_id]->pool[t_pos];
		rd_tpl[thread_id]->pool[t_pos].request = now;
		rd_tpl[thread_id]->pool[t_pos].state = 0;
		rd_tpl[thread_id]->pool[t_pos].belong = thread_id;
		rd_tpl[thread_id]->pool[t_pos].tp = READ;
		// fprintf(stderr, "working thread #%d request %llu task %d\n",\
		 thread_id, rd_tpl[thread_id]->pool[t_pos].request->private, t_pos);
		
		s_pos = query_bitmap( rd_memgt->send[thread_id] );
		s_pos += rd_memgt->send[thread_id]->size*thread_id;
		memcpy( rd_memgt->send_buffer+s_pos*buffer_per_size, &now->private, sizeof(ull) );
		memcpy( rd_memgt->send_buffer+s_pos*buffer_per_size+sizeof(ull), now->sl, sizeof(struct ScatterList) );
		
		while( qp_query(thread_id*qp_num+count%qp_num+rd_qpmgt->data_num, READ) != 3 ){
			count ++;
		}
		int tmp_qp_id = thread_id*qp_num+count%qp_num+rd_qpmgt->data_num;
		count ++;
		rd_tpl[thread_id]->pool[t_pos].qp_id = tmp_qp_id;
		rd_tpl[thread_id]->pool[t_pos].resend_count = 1;
		rd_tpl[thread_id]->pool[t_pos].send_id = s_pos;
		
		//将t_pos高三位压成thread_id
		
		inc_qp_count( rd_qpmgt, tmp_qp_id );
		post_send(tmp_qp_id, &rd_tpl[thread_id]->pool[t_pos], s_pos*buffer_per_size, buffer_per_size, \
		t_pos|((uint)thread_id<<32-3), READ);
		//fprintf(stderr, "working thread #%d submit scatter %04d qp: %d %d\n",\
		thread_id, s_pos, tmp_qp_id, *(int *)spl->pool[s_pos].task[i]->request->sl->address);
		// for( int i = 0; i < spl->pool[s_pos].number; i ++ ){
			// fprintf(stderr, " %d", *(int *)spl->pool[s_pos].task[i]->request->sl->address );
		// }
		// fprintf(stderr, "\n");
		// fprintf(stderr, "\n");
		DEBUG("working thread #%d send task %04u rid %llu qp %02d buffer %d\n", \
		thread_id, t_pos, now->private, tmp_qp_id, s_pos);
		
		now->into = elapse_sec();
		//usleep(work_timeout);
	}
}

void *rd_completion_active()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	int i, j, k, count = 0, num, reback;
	int data[128];
	fprintf(stderr, "completion thread read ready\n");
	while(1){
		TEST_NZ(ibv_get_cq_event(s_ctx->rd_comp_channel, &cq, &ctx));
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
			DEBUG("%04d!!!\n", num);
			for( k = 0; k < num; k ++ ){
				wc = &wc_array[k];
				//printf("opcode %d status %d\n", wc->opcode, wc->status);
				// switch (wc->opcode) {
					// case IBV_WC_RECV_RDMA_WITH_IMM: fprintf(stderr, "IBV_WC_RECV_RDMA_WITH_IMM\n"); break;
					// case IBV_WC_RDMA_WRITE: fprintf(stderr, "IBV_WC_RDMA_WRITE\n"); break;
					// case IBV_WC_RDMA_READ: fprintf(stderr, "IBV_WC_RDMA_READ\n"); break;
					// case IBV_WC_SEND: fprintf(stderr, "IBV_WC_SEND\n"); break;
					// case IBV_WC_RECV: fprintf(stderr, "IBV_WC_RECV\n"); break;
					// default : fprintf(stderr, "unknwon\n"); break;
				// }
				if( wc->opcode == IBV_WC_SEND ){
					struct task_active *now;
					now = ( struct task_active * )wc->wr_id;
					if( wc->status != IBV_WC_SUCCESS ){
						if( now->resend_count >= resend_limit ){
							//fprintf(stderr, "task %d wrong after resend %d times\n", \
							((ull)now-(ull)rd_tpl->pool)/sizeof(struct task_active), now->resend_count);
							// can add sth to avoid the death of this scatter
							now->state = -1;
							//fprintf(stderr, "request %llu failure\n", now->request->private);
							clean_task(now, READ);
						}
						else{
							now->resend_count ++;
							while( re_qp_query(count%rd_qpmgt->ctrl_num+rd_qpmgt->data_num, READ) != 3 ){
								count ++;
							}
							now->qp_id = count%rd_qpmgt->ctrl_num+rd_qpmgt->data_num;
							count ++;
							
							uint t_pos = ( (ull)now-(ull)rd_tpl[now->belong]->pool )/sizeof(struct task_active);
							post_send(now->qp_id , now, now->send_id*buffer_per_size, buffer_per_size, \
							t_pos|((uint)now->belong<<32-3), READ);
							//fprintf(stderr, "completion thread resubmit task %d #%d\n", \
							((ull)now-(ull)rd_tpl->pool)/sizeof(struct task_active), now->resend_count);
						}
						continue;
					}
					now->state = 1;
					now->request->tran = elapse_sec();
					/* clean send buffer */
					data[0] = now->send_id%rd_memgt->send[now->belong]->size;
					update_bitmap( rd_memgt->send[now->belong], data, 1 );
					DEBUG("send success task rid %llu buffer %d\n", now->request->private, data[0]);
				}
				
				if( wc->opcode == IBV_WC_RECV ){
					if( wc->status != IBV_WC_SUCCESS ){
						if( re_qp_query(wc->wr_id/recv_imm_data_num+rd_qpmgt->data_num, READ) == 3 ){
							post_recv( wc->wr_id/recv_imm_data_num+rd_qpmgt->data_num,\
							wc->wr_id, 0, 0, READ );
						}
						continue;
					}
					if( qp_query(wc->wr_id/recv_imm_data_num+rd_qpmgt->data_num, READ) == 3 )
						post_recv( wc->wr_id/recv_imm_data_num+rd_qpmgt->data_num,\
						wc->wr_id, 0, 0, READ );
					
					struct task_active *now;
					uint belong = (uint)wc->imm_data>>(32-3);
					uint id = wc->imm_data-belong;
					DEBUG("get CQE task t_pos %d belong %d\n", id, belong);
					now = &rd_tpl[belong]->pool[id];
					
					DEBUG("get CQE task t_pos %d belong %d request %llu\n", id, belong, now->request->private);
					
					now->state = 2;
					now->request->callback(now->request);
					dec_qp_count( rd_qpmgt, now->qp_id );
					
					TEST_NZ(clean_task(now, READ));
					back_count ++;
					//end_time = elapse_sec();
				}
			}
			//if( tot >= 150 ){ tot = 0; break; }
		}
	}
}

void huawei_syn_send( struct request_active *rq )
{
	enum type tp = evaluation();
	struct request_buffer *rbf;
	struct thread_pool *thpl;
	if( tp == WRITE ){
		rbf = wt_rbf;
		thpl = wt_thpl;
	}
	else{
		rbf = rd_rbf;
		thpl = rd_thpl;
	}
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
	rq->into = elapse_sec();
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

int e_count = 0;

enum type evaluation()
{
	enum type tp;
	if( e_count%100 < read_rate ) 
		tp = READ;
	else tp = WRITE;
	//tp = READ;
	//tp = WRITE;
	e_count ++;
	return tp;
}
