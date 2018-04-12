#include "common.h"

struct ScatterList_pool
{
	struct ScatterList pool[8192];
	uint bit[8192/32];
};

struct request_pool
{
	struct request_backup pool[8192];
	uint bit[8192/32];
};

struct package_pool
{
	struct package_backup pool[8192];
	uint bit[8192/32];
};

struct sockaddr_in6 addr;
struct ScatterList_pool *SLpl;
struct request_pool *rpl;
struct package_pool *ppl;
pthread_t completion_id;
int nofity_number;

void initialize_backup();
int on_event(struct rdma_cm_event *event, int tid);
void *completion_backup();
void commit( struct request_backup *request );
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

void initialize_backup()
{
	end = 1;
	nofity_number = 0;
	int port = 0;
	memset(&addr, 0, sizeof(addr));
	addr.sin6_family = AF_INET6;
	memgt = ( struct memory_management * ) malloc( sizeof( struct memory_management ) );
	qpmgt = ( struct qp_management * ) malloc( sizeof( struct qp_management ) );
	struct ibv_wc wc;
	int i = 0, j;
	for( i = 0; i < connect_number; i ++ ){
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
			memcpy( memgt->send_buffer, &port, sizeof(int) );
			//fprintf(stderr, "port#%d: %d\n", i, *((int *)memgt->send_buffer));
			post_send( 0, port, memgt->send_buffer, sizeof(int), 0 );
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
	memcpy( memgt->send_buffer, memgt->rdma_recv_mr, sizeof(struct ibv_mr) );
	post_send( 0, 50, memgt->send_buffer, sizeof(struct ibv_mr), 0 );
	TEST_NZ( get_wc( &wc ) );
	
	printf("add: %p length: %d\n", memgt->rdma_recv_mr->addr,
	memgt->rdma_recv_mr->length);
	
	for( i = 0; i < qpmgt->ctrl_num; i ++ ){
		for( j = 0; j < recv_buffer_num; j ++ ){
			post_recv( i+qpmgt->data_num, i*recv_buffer_num+j, \
			(i*recv_buffer_num+j)*buffer_per_size );
		}
	}
	
	/* initialize pool */
	rpl = ( struct request_pool * )malloc( sizeof( struct request_pool ) );
	ppl = ( struct package_pool * )malloc( sizeof( struct package_pool ) );
	SLpl = ( struct ScatterList_pool * )malloc( sizeof( struct ScatterList_pool ) );
	memset( rpl->bit, 0, sizeof(rpl->bit) );
	memset( ppl->bit, 0, sizeof(ppl->bit) );
	memset( SLpl->bit, 0, sizeof(SLpl->bit) );
	
	pthread_create( &completion_id, NULL, completion_backup, NULL );
}

void finalize_backup()
{
	
}

void *completion_backup()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc, *wc_array; 
	wc_array = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc)*105 );
	void *ctx;
	int i, j, k, r_pos, p_pos, SL_pos, cnt = 0, data[128];
	while(1){
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));
		int tot = 0;
		while(1){
			int num = ibv_poll_cq(cq, 10, wc_array);
			if( num <= 0 ) break;
			tot += num;
			// if( wc->status != IBV_WC_SUCCESS ){
				// fprintf(stderr, "wr_id: %lld wrong status %d type: ", wc->wr_id, wc->status);
				// switch (wc->opcode) {
					// case IBV_WC_RECV_RDMA_WITH_IMM: fprintf(stderr, "IBV_WC_RECV_RDMA_WITH_IMM\n"); break;
					// case IBV_WC_RDMA_WRITE: fprintf(stderr, "IBV_WC_RDMA_WRITE\n"); break;
					// case IBV_WC_RDMA_READ: fprintf(stderr, "IBV_WC_RDMA_READ\n"); break;
					// case IBV_WC_SEND: fprintf(stderr, "IBV_WC_SEND\n"); break;
					// case IBV_WC_RECV: fprintf(stderr, "IBV_WC_RECV\n"); break;
					// default : fprintf(stderr, "unknwon\n"); break;
				// }
			// }
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
						fprintf(stderr, "send failure id: %p type %d\n", wc->wr_id, wc->status);
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
					update_bit( SLpl->bit, 0, 8192, data, num );
					
					/* clean request pool */
					//printf("clean request pool\n");
					for( i = 0, num = 0; i < now->number; i ++ ){
						data[num++] = (int)( ((ull)(now->request[i])-(ull)(rpl->pool))/sizeof( struct request_backup ) );
					}
					update_bit( rpl->bit, 0, 8192, data, num );
					
					/* clean package pool */
					//printf("clean package pool\n");
					data[0] = (int)( ( (ull)now-(ull)(ppl->pool) )/sizeof( struct package_backup ) );
					update_bit( ppl->bit, 0, 8192, data, 1 );
				}
				
				if( wc->opcode == IBV_WC_RECV ){
					if( wc->status != IBV_WC_SUCCESS ){
						fprintf(stderr, "recv failure id: %llu qp: %d\n", wc->wr_id,\
						wc->wr_id/recv_buffer_num+qpmgt->data_num);
						if( qp_query(wc->wr_id/recv_buffer_num+qpmgt->data_num) == 3 )
							post_recv( wc->wr_id/recv_buffer_num+qpmgt->data_num, wc->wr_id, wc->wr_id*buffer_per_size );
						continue;
					}
					
					void *content;
					content = memgt->recv_buffer+wc->wr_id*buffer_per_size;//attention to start of buffer
					uint package_id = *(uint *)content;
					content += sizeof( uint );
					
					int number = *(int *)content, scatter_size, package_total = 0;
					content += sizeof(int);
					
					p_pos = query_bit_free( ppl->bit, 0, 8192 );
					ppl->pool[p_pos].num_finish = 0;
					ppl->pool[p_pos].package_active_id = package_id;
					fprintf(stderr, "get CQE package %d scatter_num %d qp %d local %p\n", \
					package_id, number, wc->wr_id/recv_buffer_num+qpmgt->data_num, &ppl->pool[p_pos]);
					
					for( i = 0; i < number; i ++ ){
						scatter_size = *(int *)content;
						content += sizeof(int);
						for( j = 0; j < scatter_size; j ++ ){
							struct ScatterList *sclist;
							/* initialize request */
							r_pos = query_bit_free( rpl->bit, 0, 8192 );
							//printf("get r_pos %d\n", r_pos);
							rpl->pool[r_pos].package = &ppl->pool[p_pos];
							ppl->pool[p_pos].request[package_total++] = &rpl->pool[r_pos];
							rpl->pool[r_pos].private = *(void **)content;
							content += sizeof(void *);
							
							sclist = content;
							SL_pos = query_bit_free( SLpl->bit, 0, 8192 );
							//printf("get SL_pos %d\n", SL_pos);
							SLpl->pool[SL_pos].next = NULL;
							SLpl->pool[SL_pos].address = sclist->address;
							SLpl->pool[SL_pos].length = sclist->length;
							rpl->pool[r_pos].sl = &SLpl->pool[SL_pos];
							content += sizeof( struct ScatterList );
							//printf("add %p len %d\n", SLpl->pool[SL_pos].address, SLpl->pool[SL_pos].length);
						}
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
			if( tot >= 25 ){ tot = 0; break; }
		}
	}
}
int data[1<<15], num = 0;
void commit( struct request_backup *request )
{
	//printf("request %p sl %p add %p\n", request, request->sl, request->sl->address);
	fprintf(stderr, "commit request %d addr %p len %d r_id %llu\n", \
	( (ull)request-(ull)rpl->pool )/sizeof(struct request_backup), \
	request->sl->address, request->sl->length, request->private);
	data[num++] = *(int *)request->sl->address;
	notify( request );
	//printf("commit end\n");
}

void notify( struct request_backup *request )
{
	request->package->num_finish ++;
	/* 回收空间 */
	if( request->package->num_finish == request->package->number ){
		//printf("send ok\n");
		
		while( qp_query(nofity_number%qpmgt->ctrl_num+qpmgt->data_num) != 3 ) nofity_number ++;
		
		post_send( nofity_number%qpmgt->ctrl_num+qpmgt->data_num, request->package,\
		memgt->send_buffer, 0, request->package->package_active_id );
		
		fprintf(stderr, "send package ack local %p qp %d\n", \
		request->package, nofity_number%qpmgt->ctrl_num+qpmgt->data_num);
		
		nofity_number ++;
	}
}

int cmp( const void *a, const void *b )
{
	return *(int *)a > *(int *)b ? 1 : -1;
}

int main()
{
	initialize_backup();
	fprintf(stderr, "BUFFER_SIZE %d recv_buffer_num %d buffer_per_size %d ctrl_number %d\n",\
		BUFFER_SIZE, recv_buffer_num, buffer_per_size, ctrl_number);
	if( BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number ) {
		fprintf(stderr, "BUFFER_SIZE < recv_buffer_num*buffer_per_size*ctrl_number\n");
		exit(1);
	}
	sleep(15);
	TEST_NZ(pthread_cancel(completion_id));
	TEST_NZ(pthread_join(completion_id, NULL));
	qsort( data, num, sizeof(int), cmp );
	printf("recv num: %d\n", num);
	//for( int i = 0; i < num; i ++ ) printf("%d\n", data[i]);
}