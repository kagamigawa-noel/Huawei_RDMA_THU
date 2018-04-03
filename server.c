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
	int port = 0;
	memset(&addr, 0, sizeof(addr));
	addr.sin6_family = AF_INET6;
	memgt = ( struct memory_management * ) malloc( sizeof( struct memory_management ) );
	qpmgt = ( struct qp_management * ) malloc( sizeof( struct qp_management ) );
	struct ibv_wc wc;
	int i = 0;
	for( i = 0; i < 10; i ++ ){
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
			post_send( 0, port, sizeof(int), 0 );
			printf("post send ok\n");
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
	post_send( 0, 50, sizeof(struct ibv_mr), 0 );
	TEST_NZ( get_wc( &wc ) );
	
	printf("add: %p length: %d\n", memgt->rdma_recv_mr->addr,
	memgt->rdma_recv_mr->length);
	
	for( i = 0; i < 10; i ++ ){
		post_recv( 0, 0, i*128 );
	}
	
	/* initialize pool */
	rpl = ( struct request_pool * )malloc( sizeof( struct request_pool ) );
	ppl = ( struct package_pool * )malloc( sizeof( struct package_pool ) );
	SLpl = ( struct ScatterList_pool * )malloc( sizeof( struct ScatterList_pool ) );
	
	pthread_create( &completion_id, NULL, completion_backup, NULL );
}

void *completion_backup()
{
	struct ibv_cq *cq;
	struct ibv_wc *wc; wc = ( struct ibv_wc * )malloc( sizeof(struct ibv_wc) );
	void *ctx;
	int i, j, r_pos, p_pos, SL_pos, cnt = 0;
	while(1){
		TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));
		while(ibv_poll_cq(cq, 1, wc)){
			if( wc->status != IBV_WC_SUCCESS ){
				fprintf(stderr, "wr_id: %lld wrong type: ", wc->wr_id);
				switch (wc->opcode) {
					case IBV_WC_RECV_RDMA_WITH_IMM: fprintf(stderr, "IBV_WC_RECV_RDMA_WITH_IMM\n"); break;
					case IBV_WC_RDMA_WRITE: fprintf(stderr, "IBV_WC_RDMA_WRITE\n"); break;
					case IBV_WC_RDMA_READ: fprintf(stderr, "IBV_WC_RDMA_READ\n"); break;
					case IBV_WC_SEND: fprintf(stderr, "IBV_WC_SEND\n"); break;
					case IBV_WC_RECV: fprintf(stderr, "IBV_WC_RECV\n"); break;
					default : fprintf(stderr, "unknwon\n"); break;
				}
			}
			
			if( wc->opcode == IBV_WC_SEND ){
				
			}
			
			if( wc->opcode == IBV_WC_RECV ){
				// maybe promblem
				//post_recv( wc->wr_id, wc->wr_id, wc->wr_id*128 );
				
				void *content;
				content = memgt->recv_buffer;
				uint package_id = *(uint *)content;
				content += sizeof( uint );
				
				int number = *(int *)content;
				content += sizeof(int);
				
				p_pos = query_bit_free( ppl->bit, 8192/32 );
				ppl->pool[p_pos].num_finish = 0;
				ppl->pool[p_pos].number = number;
				ppl->pool[p_pos].package_active_id = package_id;
				
				for( i = 0; i < number; i ++ ){
					struct ScatterList *sclist;
					sclist = content;
					// to commit
					/* initialize request */
					r_pos = query_bit_free( rpl->bit, 8192/32 );
					rpl->pool[r_pos].package = &ppl->pool[p_pos];
					
					SL_pos = query_bit_free( SLpl->bit, 8192/32 );
					SLpl->pool[SL_pos].next = NULL;
					SLpl->pool[SL_pos].address = sclist->address;
					SLpl->pool[SL_pos].length = sclist->length;
					rpl->pool[r_pos].sl = &SLpl->pool[SL_pos];
					
					commit( &rpl->pool[r_pos] );
					
					content += sizeof( struct ScatterList );
				}
				
				post_recv( wc->wr_id, wc->wr_id, wc->wr_id*128 );
			}
		}
	}
}

void commit( struct request_backup *request )
{
	notify( request );
}

void notify( struct request_backup *request )
{
	request->package->num_finish ++;
	/* 回收空间 */
	if( request->package->num_finish == request->package->number ){
		post_send( 0, 0, 0, request->package->package_active_id );
	}
}

int main()
{
	initialize_backup();
	sleep(100);
}