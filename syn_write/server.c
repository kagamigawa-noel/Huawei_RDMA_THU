#include "common.h"

struct ScatterList_pool
{
	struct ScatterList *pool;
	struct bitmap *btmp;
};

struct request_pool
{
	struct request_backup *pool;
	struct bitmap *btmp;
};

struct task_pool
{
	struct task_backup *pool;
	struct bitmap *btmp;
};

struct sockaddr_in6 addr;
struct ScatterList_pool *rd_SLpl, *wt_SLpl;
struct request_pool *rd_rpl, *wt_rpl;
struct task_pool *rd_tpl, *wt_tpl;
pthread_t completion_id[2];
int nofity_number;

ull data[1<<15];
int num = 0;
double ave_lat, base;

void initialize_backup();
int on_event(struct rdma_cm_event *event, int tid);
void *wt_completion_backup();
void *rd_completion_backup();
void (*commit)( struct request_backup *request );
void notify( struct request_backup *request );
int clean_task( struct task_backup *now, enum type tp );

int on_event(struct rdma_cm_event *event, int tid)
{
	int r = 0;
	if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)
	  r = on_connect_request(event->id, tid);
	else if (event->event == RDMA_CM_EVENT_ESTABLISHED)
	  r = on_connection(event->id, tid);
	// else if (event->event == RDMA_CM_EVENT_DISCONNECTED)
	  // r = on_disconnect(event->id);
	else
	  die("on_event: unknown event.");

	return r;
}

int clean_task( struct task_backup *now, enum type tp )
{
	int data[10], num = 0, reback = 0;
	struct ScatterList_pool *SLpl;
	struct request_pool *rpl;
	struct task_pool *tpl;
	if( tp == READ ){
		SLpl = rd_SLpl;
		rpl = rd_rpl;
		tpl = rd_tpl;
	}
	else{
		SLpl = wt_SLpl;
		rpl = wt_rpl;
		tpl = wt_tpl;
	}
	/* clean ScatterList pool */
	//printf("clean ScatterList pool\n");
	data[0] = (int)( ((ull)(now->request->sl)-(ull)(SLpl->pool))/sizeof( struct ScatterList ) );
	update_bitmap( SLpl->btmp, data, 1 );
	
	/* clean request pool */
	//printf("clean request pool\n");
	data[0] = (int)( ((ull)(now->request)-(ull)(rpl->pool))/sizeof( struct request_backup ) );
	update_bitmap( rpl->btmp, data, 1 );
	
	/* clean task pool */
	//printf("clean task pool\n");
	data[0] = (int)( ( (ull)now-(ull)(tpl->pool) )/sizeof( struct task_backup ) );
	update_bitmap( tpl->btmp, data, 1 );
	
	return 0;
}

int clean_buffer( struct task_backup *now, enum type tp )
{
	int data[10], num = 0, reback = 0;
	struct memory_management *memgt;
	if( tp == READ ){
		memgt = rd_memgt;
	}
	else{
		return 0;
		memgt = wt_memgt;
	}
	data[0] = (int)( ((ull)now->local_sge.address-(ull)memgt->rdma_recv_region)/request_size );
	reback = update_bitmap( memgt->peer[0], data, 1 );
	return 0;
}

void initialize_backup( void (*f)(struct request_backup *request) )
{
	end = 1;
	nofity_number = 0;
	int port = 0;
	memset(&addr, 0, sizeof(addr));
	addr.sin6_family = AF_INET6;
	s_ctx = ( struct connection * )malloc( sizeof( struct connection ) );
	rd_memgt = ( struct memory_management * ) malloc( sizeof( struct memory_management ) );
	wt_memgt = ( struct memory_management * ) malloc( sizeof( struct memory_management ) );
	rd_qpmgt = ( struct qp_management * ) malloc( sizeof( struct qp_management ) );
	wt_qpmgt = ( struct qp_management * ) malloc( sizeof( struct qp_management ) );
	struct ibv_wc wc;
	int i = 0, j;
	for( i = 0; i < connect_number*2; i ++ ){
		TEST_Z(ec = rdma_create_event_channel());
		TEST_NZ(rdma_create_id(ec, &listener[i], NULL, RDMA_PS_TCP));
		TEST_NZ(rdma_bind_addr(listener[i], (struct sockaddr *)&addr));
		TEST_NZ(rdma_listen(listener[i], 10)); /* backlog=10 is arbitrary */
		port = ntohs(rdma_get_src_port(listener[i]));
		fprintf(stderr, "port#%d: %d\n", i, port);
		if( i == 0 ){
			printf("listening on port %d.\n", port);
		}
		else{
			//fprintf(stderr, "port#%d: %d\n", i, *((int *)memgt->send_buffer));
			post_send( 0, i, 0, 0, port, READ );
			//printf("post send ok\n");
			TEST_NZ( get_wc( &wc, READ ) );
		}
		
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
	memcpy( wt_memgt->send_buffer, wt_memgt->rdma_recv_mr, sizeof(struct ibv_mr) );
	post_send( 0, 0, 0, sizeof(struct ibv_mr), 0, WRITE );
	TEST_NZ( get_wc( &wc, WRITE ) );
	printf("write add: %p length: %d\n", wt_memgt->rdma_recv_mr->addr,
	wt_memgt->rdma_recv_mr->length);
	
	post_recv( 0, 0, 0, sizeof(struct ibv_mr), READ );
	TEST_NZ( get_wc( &wc, READ ) );
	memcpy( &rd_memgt->peer_mr, rd_memgt->recv_buffer, sizeof(struct ibv_mr) );
	printf("read add: %p length: %d\n", rd_memgt->peer_mr.addr,
	rd_memgt->peer_mr.length);
	
	for( i = 0; i < wt_qpmgt->data_num; i ++ ){
		for( j = 0; j < recv_imm_data_num; j ++ ){
			post_recv( i, i*recv_imm_data_num+j, \
			0, 0, WRITE );
		}
	}
	
	// for( i = wt_qpmgt->data_num; i < wt_qpmgt->data_num+wt_qpmgt->ctrl_num; i ++ ){
		// for( j = 0; j < recv_imm_data_num; j ++ ){
			// post_recv( i, i*recv_imm_data_num+j, \
			// ((i-wt_qpmgt->data_num)*recv_imm_data_num+j)*buffer_per_size, buffer_per_size, WRITE );
		// }
	// }
	//wr_id 连续, buffer从0开始
	
	for( i = 0; i < rd_qpmgt->ctrl_num; i ++ ){
		for( j = 0; j < recv_buffer_num; j ++ ){
			post_recv( rd_qpmgt->data_num+i, i*recv_buffer_num+j, \
			(i*recv_buffer_num+j)*buffer_per_size, buffer_per_size, READ );
		}
	}
	
	/* initialize pool */
	rd_rpl = ( struct request_pool * )malloc( sizeof( struct request_pool ) );
	rd_SLpl = ( struct ScatterList_pool * )malloc( sizeof( struct ScatterList_pool ) );
	rd_tpl = ( struct task_backup * )malloc( sizeof( struct task_backup ) );
	
	rd_rpl->pool = ( struct request_backup * )malloc( sizeof(struct request_backup)*request_pool_size );
	rd_SLpl->pool = ( struct ScatterList * )malloc( sizeof(struct ScatterList)*ScatterList_pool_size );
	rd_tpl->pool = ( struct task_backup * )malloc( sizeof(struct task_backup)*task_pool_size );
	
	init_bitmap(&rd_rpl->btmp, request_pool_size);
	init_bitmap(&rd_SLpl->btmp, ScatterList_pool_size);
	init_bitmap(&rd_tpl->btmp, task_pool_size);
	
	wt_rpl = ( struct request_pool * )malloc( sizeof( struct request_pool ) );
	wt_SLpl = ( struct ScatterList_pool * )malloc( sizeof( struct ScatterList_pool ) );
	wt_tpl = ( struct task_backup * )malloc( sizeof( struct task_backup ) );
	
	wt_rpl->pool = ( struct request_backup * )malloc( sizeof(struct request_backup)*request_pool_size );
	wt_SLpl->pool = ( struct ScatterList * )malloc( sizeof(struct ScatterList)*ScatterList_pool_size );
	wt_tpl->pool = ( struct task_backup * )malloc( sizeof(struct task_backup)*task_pool_size );
	
	init_bitmap(&wt_rpl->btmp, request_pool_size);
	init_bitmap(&wt_SLpl->btmp, ScatterList_pool_size);
	init_bitmap(&wt_tpl->btmp, task_pool_size);
	
	init_bitmap(&rd_memgt->peer[0], RDMA_BUFFER_SIZE/request_size);
	
	commit = f;
	
	pthread_create( &completion_id[0], NULL, rd_completion_backup, NULL );
	pthread_create( &completion_id[1], NULL, wt_completion_backup, NULL );
	sleep(2);
}

void finalize_backup()
{
	printf("start finalize\n");
	TEST_NZ(pthread_cancel(completion_id[0]));
	TEST_NZ(pthread_join(completion_id[0], NULL));
	
	TEST_NZ(pthread_cancel(completion_id[1]));
	TEST_NZ(pthread_join(completion_id[1], NULL));
	
	/* destroy ScatterList pool */
	free(rd_SLpl->pool); rd_SLpl->pool = NULL;
	final_bitmap(rd_SLpl->btmp);
	free(rd_SLpl); rd_SLpl = NULL;
	
	free(wt_SLpl->pool); wt_SLpl->pool = NULL;
	final_bitmap(wt_SLpl->btmp);
	free(wt_SLpl); wt_SLpl = NULL;
	fprintf(stderr, "destroy ScatterList pool success\n");
		
	/* destroy request pool */
	free(rd_rpl->pool); rd_rpl->pool = NULL;
	final_bitmap(rd_rpl->btmp);
	free(rd_rpl); rd_rpl = NULL;
	
	free(wt_rpl->pool); wt_rpl->pool = NULL;
	final_bitmap(wt_rpl->btmp);
	free(wt_rpl); wt_rpl = NULL;
	fprintf(stderr, "destroy request pool success\n");
	
	/* destroy task pool */
	free(rd_tpl->pool); rd_tpl->pool = NULL;
	final_bitmap(rd_tpl->btmp);
	free(rd_tpl); rd_tpl = NULL;
	
	free(wt_tpl->pool); wt_tpl->pool = NULL;
	final_bitmap(wt_tpl->btmp);
	free(wt_tpl); wt_tpl = NULL;
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

void *wt_completion_backup()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	int i, j, k, r_pos, t_pos, SL_pos, cnt = 0, data[128], tot = 0, send_count = 0;
	while(1){
		TEST_NZ(ibv_get_cq_event(s_ctx->wt_comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));
		tot = 0;
		while(1){
			int num = ibv_poll_cq(cq, 100, wc_array);
			if( num <= 0 ) break;
			tot += num;
			//fprintf(stderr, "%04d CQE get!!!\n", num);
			for( k = 0; k < num; k ++ ){
				wc = &wc_array[k];
				if( wc->opcode == IBV_WC_SEND ){
					if( wc->status != IBV_WC_SUCCESS ){
						
						fprintf(stderr, "send failure id: %d type %d\n",\
						(wc->wr_id-(ull)wt_tpl->pool)/sizeof(struct task_backup), wc->status);
						// if( (wc->wr_id-(ull)ppl->pool)/sizeof(struct package_backup) < 0 || \
						// (wc->wr_id-(ull)ppl->pool)/sizeof(struct package_backup) >= package_pool_size )
							// continue;
						// struct package_backup *now;
						// now = ( struct package_backup * )wc->wr_id;
						// if( now->resend_count >= resend_limit ){
							// clean_package(now);
						// }
						continue;
					}
					
					struct task_backup *now;
					now = ( struct task_backup * )wc->wr_id;
					
					now->request->ed = elapse_sec();
					ave_lat += now->request->ed-now->request->st;
					DEBUG("%lf %lf %lf\n", now->request->st-base, now->request->ed-base, now->request->ed-now->request->st);
					DEBUG("get CQE task_active_id %u back ack\n", now->task_active_id);
					clean_task(now, WRITE);
				}
				
				if( wc->opcode == IBV_WC_RECV || wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM ){
					if( wc->status != IBV_WC_SUCCESS ){
						fprintf(stderr, "recv failure id: %llu qp: %d\n", wc->wr_id,\
						wc->wr_id/recv_imm_data_num);
						if( re_qp_query(wc->wr_id/recv_imm_data_num, WRITE) == 3 )
							post_recv( wc->wr_id/recv_imm_data_num, wc->wr_id, 0, 0, WRITE );
						continue;
					}
					double tmp_time = elapse_sec();
					if( qp_query(wc->wr_id/recv_imm_data_num, WRITE) == 3 )
						post_recv( wc->wr_id/recv_imm_data_num, wc->wr_id, 0, 0, WRITE );
					
					void *content;
					content = wt_memgt->rdma_recv_region+wc->imm_data*(request_size+metedata_size);//attention to start of buffer
					ull task_active_id = *(ull *)content;
					content += sizeof( ull );
					
					ull private = *(ull *)content;
					content += sizeof( ull );
					
					t_pos = query_bitmap( wt_tpl->btmp );
					if( t_pos==-1 ){
						fprintf(stderr, "no more space while finding task_pool\n");
						exit(1);
					}
					wt_tpl->pool[t_pos].resend_count = 0;
					wt_tpl->pool[t_pos].task_active_id = task_active_id;
					wt_tpl->pool[t_pos].state = 0;
					wt_tpl->pool[t_pos].tp = WRITE;
					DEBUG("get CQE task %llu task_private %llu qp %d\n", \
					task_active_id, private, wc->wr_id/recv_imm_data_num);
					
					r_pos = query_bitmap( wt_rpl->btmp );
					SL_pos = query_bitmap( wt_SLpl->btmp );
					
					wt_rpl->pool[r_pos].private = private;
					wt_rpl->pool[r_pos].sl = &wt_SLpl->pool[SL_pos];
					wt_rpl->pool[r_pos].task = &wt_tpl->pool[t_pos];
					wt_rpl->pool[r_pos].st = tmp_time;
					
					wt_SLpl->pool[SL_pos].next = NULL;
					wt_SLpl->pool[SL_pos].address = wt_memgt->rdma_recv_region+wc->imm_data*(request_size+metedata_size)+metedata_size;
					wt_SLpl->pool[SL_pos].length = request_size;
					
					wt_tpl->pool[t_pos].request = &wt_rpl->pool[r_pos];
					/* not used */
					//memcpy( &wt_tpl->pool[t_pos].local_sge, &wt_SLpl->pool[SL_pos], sizeof(struct ScatterList) );
					DEBUG("get task %llu\n", wt_rpl->pool[r_pos].private);
					commit(&wt_rpl->pool[r_pos]);
				}
			}
			//if( tot >= 250 ) tot -= num;
		}
	}
}

void *rd_completion_backup()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	int i, j, k, r_pos, t_pos, SL_pos, p_pos, cnt = 0, data[128], tot = 0, send_count = 0, count = 0, reback = 0;
	while(1){
		TEST_NZ(ibv_get_cq_event(s_ctx->rd_comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));
		tot = 0;
		while(1){
			int num = ibv_poll_cq(cq, 100, wc_array);
			if( num <= 0 ) break;
			tot += num;
			//fprintf(stderr, "%04d!!!\n", num);
			for( k = 0; k < num; k ++ ){
				wc = &wc_array[k];
				//printf("opcode %d status %d\n", wc->opcode, wc->status);
				if( wc->opcode == IBV_WC_SEND ){
					if( wc->status != IBV_WC_SUCCESS ){
						
						fprintf(stderr, "send failure id: %d type %d\n",\
						(wc->wr_id-(ull)rd_tpl->pool)/sizeof(struct task_backup), wc->status);
						// if( (wc->wr_id-(ull)ppl->pool)/sizeof(struct package_backup) < 0 || \
						// (wc->wr_id-(ull)ppl->pool)/sizeof(struct package_backup) >= package_pool_size )
							// continue;
						// struct package_backup *now;
						// now = ( struct package_backup * )wc->wr_id;
						// if( now->resend_count >= resend_limit ){
							// clean_package(now);
						// }
						continue;
					}
					struct task_backup *now;
					now = ( struct task_backup * )wc->wr_id;
					//ave_lat += elapse_sec()-now->request->st;
					DEBUG("get CQE task_active_id %u back ack\n", now->task_active_id);
					
					clean_task(now, READ);
				}
				if( wc->opcode == IBV_WC_RECV || wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM ){
					if( wc->status != IBV_WC_SUCCESS ){
						fprintf(stderr, "recv failure id: %llu qp: %d\n", wc->wr_id,\
						wc->wr_id/recv_buffer_num);
						if( re_qp_query(wc->wr_id/recv_buffer_num+rd_qpmgt->data_num, READ) == 3 )
							post_recv( wc->wr_id/recv_buffer_num+rd_qpmgt->data_num, wc->wr_id,
							wc->wr_id*buffer_per_size, buffer_per_size, READ );
						continue;
					}
					
					ull private;
					
					t_pos = query_bitmap( rd_tpl->btmp );
					if( t_pos==-1 ){
						fprintf(stderr, "no more space while finding task_pool\n");
						exit(1);
					}
					rd_tpl->pool[t_pos].resend_count = 0;
					rd_tpl->pool[t_pos].task_active_id = wc->imm_data;
					rd_tpl->pool[t_pos].state = 0;
					rd_tpl->pool[t_pos].tp = READ;
					DEBUG("wating task %llu task_private %llu t_pos %d\n", \
					wc->imm_data, private, t_pos);
					
					void *content;
					content = rd_memgt->recv_buffer+wc->wr_id*buffer_per_size;//attention to start of buffer
					private = *(ull *)content;
					content += sizeof( ull );
					
					memcpy( &rd_tpl->pool[t_pos].remote_sge, content, sizeof(struct ScatterList) );
					
					/*如果completion thread需要多个线程，则此处代码需要修改*/
					p_pos = query_bitmap( rd_memgt->peer[0] );
					
					rd_tpl->pool[t_pos].local_sge.address = rd_memgt->rdma_recv_region+p_pos*request_size;
					rd_tpl->pool[t_pos].local_sge.length = request_size;
					
					while( qp_query(count%rd_qpmgt->data_num, READ) != 3 ){
						count ++;
					}
					
					double tmp_time = elapse_sec();
					post_rdma_read( count%rd_qpmgt->data_num, &rd_tpl->pool[t_pos] );
					count ++;
					
					//fprintf(stderr, "start read task %llu remote addr %p len %d local addr %p len %d\n", private,\
					rd_tpl->pool[t_pos].remote_sge.address, rd_tpl->pool[t_pos].remote_sge.length, \
					rd_tpl->pool[t_pos].local_sge.address, rd_tpl->pool[t_pos].local_sge.length);
					
					r_pos = query_bitmap( rd_rpl->btmp );
					
					rd_rpl->pool[r_pos].private = private;
					rd_rpl->pool[r_pos].task = &rd_tpl->pool[t_pos];
					rd_rpl->pool[r_pos].st = tmp_time;
					
					rd_tpl->pool[t_pos].request = &rd_rpl->pool[r_pos];
					
					if( qp_query(wc->wr_id/recv_buffer_num+rd_qpmgt->data_num, READ) == 3 )
					post_recv( wc->wr_id/recv_buffer_num+rd_qpmgt->data_num, wc->wr_id,
						wc->wr_id*buffer_per_size, buffer_per_size, READ );
				}
				if( wc->opcode == IBV_WC_RDMA_READ ){
					struct task_backup *now;
					now = ( struct task_backup * )wc->wr_id;// need to check whether it is a pointer of pool
					if( wc->status != IBV_WC_SUCCESS ){
						if( now->resend_count >= resend_limit ){
							now->state = -1;
							fprintf(stderr, "request %llu failure\n", now->request->private);
							clean_task(now, READ);
							data[0] = ((ull)now->local_sge.address-(ull)rd_memgt->rdma_recv_region)/request_size;
							update_bitmap( rd_memgt->peer[0], data, 1 );

						}
						else{
							now->resend_count ++;
							while( re_qp_query(count%rd_qpmgt->data_num, READ) != 3 ){
								count ++;
							}
							
							post_rdma_read( count%rd_qpmgt->data_num, now );
							count ++;
							
							fprintf(stderr, "completion thread resubmit task %d #%d\n", \
							((ull)now-(ull)rd_tpl->pool)/sizeof(struct task_backup), now->resend_count);
						}
						continue;
					}
					ave_lat += elapse_sec()-now->request->st;
					now->state = 1;

					SL_pos = query_bitmap( rd_SLpl->btmp );
					
					//not necessary copy maybe
					memcpy( &rd_SLpl->pool[SL_pos], &now->local_sge, sizeof(struct ScatterList) );
					
					now->request->sl = &rd_SLpl->pool[SL_pos];
					
					DEBUG("get task rid %llu\n", now->request->private);
					commit(now->request);
					
				}
			}
			if( tot >= 250 ) tot -= num;
		}
	}
}

void notify( struct request_backup *request )
{
	request->task->resend_count ++;
	struct qp_management *qpmgt;
	struct memory_management *memgt;
	if( request->task->tp == WRITE ){ qpmgt = wt_qpmgt; memgt = wt_memgt; }
	else{ qpmgt = rd_qpmgt; memgt = rd_memgt; }
	
	if( request->task->tp == READ )
		clean_buffer( request->task, READ );
	while( qp_query(nofity_number%qpmgt->ctrl_num+qpmgt->data_num, request->task->tp) != 3 ) nofity_number ++;
	
	post_send( nofity_number%qpmgt->ctrl_num+qpmgt->data_num, request->task,\
	0, 0, request->task->task_active_id, request->task->tp );
	
	DEBUG("send task ack task_active_id %d qp %d\n", \
	request->task->task_active_id, nofity_number%qpmgt->ctrl_num+qpmgt->data_num);
	
	nofity_number ++;
}
