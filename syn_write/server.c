#include "common.h"

struct ScatterList_pool
{
	struct ScatterList *pool;
	uint *bit;
};

struct request_pool
{
	struct request_backup *pool;
	uint *bit;
};

struct sockaddr_in6 addr;
struct ScatterList_pool *rd_SLpl, *wt_SLpl;
struct request_pool *rd_rpl, *wt_rpl;
pthread_t completion_id[2];
int nofity_number;

ull data[1<<15];
int num = 0;

void initialize_backup();
int on_event(struct rdma_cm_event *event, int tid);
void *completion_backup();
void (*commit)( struct request_backup *request );
void notify( struct request_backup *request );

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

void initialize_backup( void (*f)(struct request_backup *request) )
{
	end = 1;
	nofity_number = 0;
	int port = 0;
	memset(&addr, 0, sizeof(addr));
	addr.sin6_family = AF_INET6;
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
			post_send( 0, port, rd_memgt->send_buffer, 0, port, read );
			//printf("post send ok\n");
			TEST_NZ( get_wc( &wc ) );
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
	post_send( 0, 0, wt_memgt->send_buffer, sizeof(struct ibv_mr), 0, write );
	TEST_NZ( get_wc( &wc ) );
	printf("write add: %p length: %d\n", wt_memgt->rdma_recv_mr->addr,
	wt_memgt->rdma_recv_mr->length);
	
	post_recv( 0, 0, 0, sizeof(struct ibv_mr), read );
	TEST_NZ( get_wc( &wc ) );
	memcpy( &rd_memgt->peer, rd_memgt->rdma_recv_mr, sizeof(struct ibv_mr) );
	printf("write add: %p length: %d\n", rd_memgt->rdma_recv_mr->addr,
	rd_memgt->rdma_recv_mr->length);
	
	for( i = 0; i < wt_qpmgt->data_num; i ++ ){
		for( j = 0; j < recv_buffer_num; j ++ ){
			post_recv( i, i*recv_buffer_num+j, \
			0, 0, write );
		}
	}
	
	for( i = wt_qpmgt->data_num; i < wt_qpmgt->data_num+wt_qpmgt->ctrl_num; i ++ ){
		for( j = 0; j < recv_buffer_num; j ++ ){
			post_recv( i, i*recv_buffer_num+j, \
			((i-wt_qpmgt->data_num)*recv_buffer_num+j)*buffer_per_size, buffer_per_size, write );
		}
	}//wr_id 连续, buffer从0开始
	
	for( i = 0; i < rd_qpmgt->ctrl_num; i ++ ){
		for( j = 0; j < recv_buffer_num; j ++ ){
			post_recv( rd_qpmgt->data_num+i, i*recv_buffer_num+j, \
			(i*recv_buffer_num+j)*buffer_per_size, buffer_per_size, read );
		}
	}
	
	/* initialize pool */
	rd_rpl = ( struct request_pool * )malloc( sizeof( struct request_pool ) );
	rd_SLpl = ( struct ScatterList_pool * )malloc( sizeof( struct ScatterList_pool ) );
	
	rd_rpl->pool = ( struct request_backup * )malloc( sizeof(struct request_backup)*request_pool_size );
	rd_SLpl->pool = ( struct ScatterList * )malloc( sizeof(struct ScatterList)*ScatterList_pool_size );
	
	rd_rpl->bit = ( uint * )malloc( sizeof(uint)*request_pool_size/32 );
	rd_SLpl->bit = ( uint * )malloc( sizeof(uint)*ScatterList_pool_size/32 );
	
	memset( rd_rpl->bit, 0, sizeof(rd_rpl->bit) );
	memset( rd_SLpl->bit, 0, sizeof(rd_SLpl->bit) );
	
	
	wt_rpl = ( struct request_pool * )malloc( sizeof( struct request_pool ) );
	wt_SLpl = ( struct ScatterList_pool * )malloc( sizeof( struct ScatterList_pool ) );
	
	wt_rpl->pool = ( struct request_backup * )malloc( sizeof(struct request_backup)*request_pool_size );
	wt_SLpl->pool = ( struct ScatterList * )malloc( sizeof(struct ScatterList)*ScatterList_pool_size );
	
	wt_rpl->bit = ( uint * )malloc( sizeof(uint)*request_pool_size/32 );
	wt_SLpl->bit = ( uint * )malloc( sizeof(uint)*ScatterList_pool_size/32 );
	
	memset( wt_rpl->bit, 0, sizeof(wt_rpl->bit) );
	memset( wt_SLpl->bit, 0, sizeof(wt_SLpl->bit) );
	
	commit = f;
	
	pthread_create( &completion_id[0], NULL, rd_completion_backup, NULL );
	pthread_create( &completion_id[1], NULL, wt_completion_backup, NULL );
}

void finalize_backup()
{
	TEST_NZ(pthread_cancel(completion_id[0]));
	TEST_NZ(pthread_join(completion_id[0], NULL));
	
	TEST_NZ(pthread_cancel(completion_id[1]));
	TEST_NZ(pthread_join(completion_id[1], NULL));
	
	/* destroy ScatterList pool */
	free(rd_SLpl->pool); rd_SLpl->pool = NULL;
	free(rd_SLpl->bit); rd_SLpl->bit = NULL;
	free(rd_SLpl); rd_SLpl = NULL;
	
	free(wt_SLpl->pool); wt_SLpl->pool = NULL;
	free(wt_SLpl->bit); wt_SLpl->bit = NULL;
	free(wt_SLpl); wt_SLpl = NULL;
	fprintf(stderr, "destroy ScatterList pool success\n");
		
	/* destroy request pool */
	free(rd_rpl->pool); rd_rpl->pool = NULL;
	free(rd_rpl->bit); rd_rpl->bit = NULL;
	free(rd_rpl); rd_rpl = NULL;
	
	free(wt_rpl->pool); wt_rpl->pool = NULL;
	free(wt_rpl->bit); wt_rpl->bit = NULL;
	free(wt_rpl); wt_rpl = NULL;
	fprintf(stderr, "destroy request pool success\n");
	
	/* destroy qp management */
	destroy_qp_management(read);
	destroy_qp_management(write);
	fprintf(stderr, "destroy qp management success\n");
	
	/* destroy memory management */
	destroy_memory_management(end, read);
	destroy_memory_management(end, write);
	fprintf(stderr, "destroy memory management success\n");
		
	/* destroy connection struct */
	destroy_connection(read);
	destroy_connection(write);
	fprintf(stderr, "destroy connection success\n");
	
	fprintf(stderr, "finalize end\n");
}

void *completion_backup()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	int i, j, k, r_pos, p_pos, SL_pos, cnt = 0, data[128], tot = 0, send_count = 0;
	while(1){
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));
		tot = 0;
		while(1){
			int num = ibv_poll_cq(cq, 100, wc_array);
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
				if( wc->opcode == IBV_WC_SEND ){
					if( wc->status != IBV_WC_SUCCESS ){
						
						fprintf(stderr, "send failure id: %d type %d\n",\
						(wc->wr_id-(ull)ppl->pool)/sizeof(struct package_backup), wc->status);
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
					
					struct package_backup *now;
					now = ( struct package_backup * )wc->wr_id;
					
					fprintf(stderr, "get CQE package %u back ack\n", now->package_active_id);
					
					int num = 0;
					/* clean ScatterList pool */
					//printf("clean ScatterList pool\n");
					for( i = 0, num = 0; i < now->number; i ++ ){
						data[num++] = (int)( ((ull)(now->request[i]->sl)-(ull)(SLpl->pool))/sizeof( struct ScatterList ) );
					}
					update_bit( SLpl->bit, 0, ScatterList_pool_size, data, num );
					
					/* clean request pool */
					//printf("clean request pool\n");
					for( i = 0, num = 0; i < now->number; i ++ ){
						data[num++] = (int)( ((ull)(now->request[i])-(ull)(rpl->pool))/sizeof( struct request_backup ) );
					}
					update_bit( rpl->bit, 0, request_pool_size, data, num );
					
					/* clean package pool */
					//printf("clean package pool\n");
					data[0] = (int)( ( (ull)now-(ull)(ppl->pool) )/sizeof( struct package_backup ) );
					update_bit( ppl->bit, 0, package_pool_size, data, 1 );
				}
				
				if( wc->opcode == IBV_WC_RECV ){
					if( wc->status != IBV_WC_SUCCESS ){
						fprintf(stderr, "recv failure id: %llu qp: %d\n", wc->wr_id,\
						wc->wr_id/recv_buffer_num+qpmgt->data_num);
						if( re_qp_query(wc->wr_id/recv_buffer_num+qpmgt->data_num) == 3 )
							post_recv( wc->wr_id/recv_buffer_num+qpmgt->data_num, wc->wr_id, wc->wr_id*buffer_per_size );
						continue;
					}
					
					void *content;
					content = memgt->recv_buffer+wc->wr_id*buffer_per_size;//attention to start of buffer
					uint package_id = *(uint *)content;
					content += sizeof( uint );
					
					int package_total = *(int *)content, scatter_size;
					content += sizeof(int);
					
					p_pos = query_bit_free( ppl->bit, 0, package_pool_size );
					if( p_pos==-1 ){
						fprintf(stderr, "no more space while finding package_pool\n");
						exit(1);
					}
					ppl->pool[p_pos].num_finish = 0;
					ppl->pool[p_pos].package_active_id = package_id;
					fprintf(stderr, "get CQE package %d scatter_num %d qp %d local %p\n", \
					package_id, package_total, wc->wr_id/recv_buffer_num+qpmgt->data_num, &ppl->pool[p_pos]);
					
					for( i = 0; i < package_total; i ++ ){
						struct ScatterList *sclist;
						/* initialize request */
						r_pos = query_bit_free( rpl->bit, 0, request_pool_size );
						if( r_pos==-1 ){
							fprintf(stderr, "no more space while finding request_pool\n");
							exit(1);
						}
						//printf("get r_pos %d\n", r_pos);
						rpl->pool[r_pos].package = &ppl->pool[p_pos];
						ppl->pool[p_pos].request[i] = &rpl->pool[r_pos];
						rpl->pool[r_pos].private = *(void **)content;
						content += sizeof(void *);
						
						sclist = content;
						SL_pos = query_bit_free( SLpl->bit, 0, ScatterList_pool_size );
						if( SL_pos==-1 ){
							fprintf(stderr, "no more space while finding ScatterList_pool\n");
							exit(1);
						}
						//printf("get SL_pos %d\n", SL_pos);
						SLpl->pool[SL_pos].next = NULL;
						SLpl->pool[SL_pos].address = sclist->address;
						SLpl->pool[SL_pos].length = sclist->length;
						rpl->pool[r_pos].sl = &SLpl->pool[SL_pos];
						content += sizeof( struct ScatterList );
						//printf("add %p len %d\n", SLpl->pool[SL_pos].address, SLpl->pool[SL_pos].length);
					}
					ppl->pool[p_pos].number = package_total;
					fprintf(stderr, "get CQE package %d package_total %d qp %d local %p\n", \
					package_id, package_total, wc->wr_id/recv_buffer_num+qpmgt->data_num, &ppl->pool[p_pos]);
					
					/* to commit */
					for( i = 0; i < package_total; i ++ ){
						commit( ppl->pool[p_pos].request[i] );
					}
					
					if( qp_query(wc->wr_id/recv_buffer_num+qpmgt->data_num) == 3 )
						post_recv( wc->wr_id/recv_buffer_num+qpmgt->data_num, wc->wr_id, wc->wr_id*buffer_per_size );
				}
			}
			if( tot >= 250 ) tot -= num;
		}
	}
}

// void commit( struct request_backup *request )
// {
	// //printf("request %p sl %p add %p\n", request, request->sl, request->sl->address);
	// fprintf(stderr, "commit request %d addr %p len %d r_id %llu\n", \
	// ( (ull)request-(ull)rpl->pool )/sizeof(struct request_backup), \
	// request->sl->address, request->sl->length, request->private);
	// data[num++] = (ull)request->private;
	// notify( request );
	// //printf("commit end\n");
// }

void notify( struct request_backup *request )
{
	request->package->num_finish ++;
	/* 回收空间 */
	//printf("notify %p\n", request);
	//printf("num_finish %d number %d\n", \
	request->package->num_finish, request->package->number);
	if( request->package->num_finish == request->package->number ){
		//printf("send ok\n");
		request->package->resend_count ++;
		while( qp_query(nofity_number%qpmgt->ctrl_num+qpmgt->data_num) != 3 ) nofity_number ++;
		
		post_send( nofity_number%qpmgt->ctrl_num+qpmgt->data_num, request->package,\
		memgt->send_buffer, 0, request->package->package_active_id );
		
		fprintf(stderr, "send package ack local %d qp %d\n", \
		((ull)request->package-(ull)ppl->pool)/sizeof(struct package_backup), nofity_number%qpmgt->ctrl_num+qpmgt->data_num);
		
		nofity_number ++;
	}
}
