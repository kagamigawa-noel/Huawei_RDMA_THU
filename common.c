#include "common.h"

int on_connect_request(struct rdma_cm_id *id, int tid)
{
	struct rdma_conn_param cm_params;
	if(!tid) printf("received connection request.\n");
	build_connection(id, tid);
	conn_id[tid] = id;
	build_params(&cm_params);
	TEST_NZ(rdma_accept(id, &cm_params));
	return 0;
}

int on_addr_resolved(struct rdma_cm_id *rid, int tid)
{
	if(!tid) printf("address resolved.\n");
	build_connection(rid, tid);
	conn_id[tid] = rid;
	TEST_NZ(rdma_resolve_route(rid, TIMEOUT_IN_MS));
	return 0;
}

int on_route_resolved(struct rdma_cm_id *id, int tid)
{
	struct rdma_conn_param cm_params;
	if(!tid) printf("route resolved.\n");
	build_params(&cm_params);
	TEST_NZ(rdma_connect(id, &cm_params));
	
	if(!tid) printf("route resolved ok.\n");
	return 0;
}

int on_connection(struct rdma_cm_id *id, int tid)
{	
	return 1;
}

void build_connection(struct rdma_cm_id *id, int tid)
{
	struct ibv_qp_init_attr *qp_attr;
	qp_attr = ( struct ibv_qp_init_attr* )malloc( sizeof( struct ibv_qp_init_attr ) );
	if( !tid ){
	  build_context(id->verbs);
	  qpmgt->wrong_number = 0;
	  //sth need to init for 1st time
	}
	memset(qp_attr, 0, sizeof(*qp_attr));
	
	qp_attr->qp_type = IBV_QPT_RC;
	qp_attr->send_cq = s_ctx->cq;
	qp_attr->recv_cq = s_ctx->cq;

	qp_attr->cap.max_send_wr = 100;
	qp_attr->cap.max_recv_wr = 100;
	qp_attr->cap.max_send_sge = 10;
	qp_attr->cap.max_recv_sge = 10;
	
	TEST_NZ(rdma_create_qp(id, s_ctx->pd, qp_attr));
	qpmgt->qp[tid] = id->qp;
	qpmgt->qp_state[tid] = 0;
	
	if( !tid )
		register_memory( end );
}

void build_context(struct ibv_context *verbs)
{
	if (s_ctx) {
	  if (s_ctx->ctx != verbs)
		die("cannot handle events in more than one context.");
	  return;
	}
	s_ctx = ( struct connection * )malloc( sizeof( struct connection ) );
	
	s_ctx->ctx = verbs;

	TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
	TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
	TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
	TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));
	
}

void build_params(struct rdma_conn_param *params)
{
	memset(params, 0, sizeof(*params));

	params->initiator_depth = params->responder_resources = 1;
	params->rnr_retry_count = 7; /* infinite retry */
}

void register_memory( int tid )// 0 active 1 backup
{
	memgt->recv_buffer = (char *)malloc(BUFFER_SIZE);
	TEST_Z( memgt->recv_mr = ibv_reg_mr( s_ctx->pd, memgt->recv_buffer,
	BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE ) );
	
	memgt->send_buffer = (char *)malloc(BUFFER_SIZE);
	TEST_Z( memgt->send_mr = ibv_reg_mr( s_ctx->pd, memgt->send_buffer,
	BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE ) );
	
	if( tid == 1 ){
		memgt->rdma_recv_region = (char *)malloc(RDMA_BUFFER_SIZE);
		TEST_Z( memgt->rdma_recv_mr = ibv_reg_mr( s_ctx->pd, memgt->rdma_recv_region,
		RDMA_BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE ) );
	}
	
	if( tid == 1 ){
		memgt->rdma_send_region = (char *)malloc(RDMA_BUFFER_SIZE);
		TEST_Z( memgt->rdma_send_mr = ibv_reg_mr( s_ctx->pd, memgt->rdma_send_region,
		RDMA_BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE ) );
	}
	else{
		TEST_Z( memgt->rdma_send_mr = ibv_reg_mr( s_ctx->pd, memgt->application.address,
		memgt->application.length, IBV_ACCESS_LOCAL_WRITE ) );
	}
	
	memgt->send_bit = (uint *)malloc(RDMA_BUFFER_SIZE/32*4);
	memgt->recv_bit = (uint *)malloc(RDMA_BUFFER_SIZE/32*4);
	memgt->peer_bit = (uint *)malloc(RDMA_BUFFER_SIZE/32*4);
	memset( memgt->send_bit, 0, sizeof(RDMA_BUFFER_SIZE/32*4) );
	memset( memgt->recv_bit, 0, sizeof(RDMA_BUFFER_SIZE/32*4) );
	memset( memgt->peer_bit, 0, sizeof(RDMA_BUFFER_SIZE/32*4) );
}

void post_recv( int qp_id, ull tid, int offset )
{
	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	
	wr.wr_id = tid;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	
	sge.addr = (uintptr_t)memgt->recv_buffer+offset;
	sge.length = BUFFER_SIZE;
	sge.lkey = memgt->recv_mr->lkey;
	
	TEST_NZ(ibv_post_recv(qpmgt->qp[qp_id], &wr, &bad_wr));
}

void post_send( int qp_id, ull tid, void *start, int send_size, int imm_data )
{
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr, 0, sizeof(wr));
	
	wr.wr_id = tid;
	wr.opcode = IBV_WR_SEND_WITH_IMM;
	wr.sg_list = &sge;
	wr.send_flags = IBV_SEND_SIGNALED;
	if( imm_data != 0 )
		wr.imm_data = imm_data;
	wr.num_sge = 1;
	
	sge.addr = (uintptr_t)start;
	sge.length = send_size;
	sge.lkey = memgt->send_mr->lkey;
	
	TEST_NZ(ibv_post_send(qpmgt->qp[qp_id], &wr, &bad_wr));
}

void post_rdma_write( int qp_id, struct scatter_active *sct )
{
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge[10];
	
	memset(&wr, 0, sizeof(wr));
	
	wr.wr_id = (uintptr_t)sct;
	wr.opcode = IBV_WR_RDMA_WRITE;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.wr.rdma.remote_addr = (uintptr_t)sct->remote_sge.address;
	wr.wr.rdma.rkey = memgt->peer_mr.rkey;
	//printf("write remote add: %p\n", sct->remote_sge.address);
	
	wr.sg_list = sge;
	wr.num_sge = sct->number;//这里假定每个request是一个scatter
	//printf("sct->number %d\n", sct->number);
	
	for( int i = 0; i < sct->number; i ++ ){
		sge[i].addr = (uintptr_t)sct->task[i]->request->sl->address;
		sge[i].length = sct->task[i]->request->sl->length;
		sge[i].lkey = memgt->rdma_send_mr->lkey;
		//fprintf(stderr, "write#%02d: %p len %d\n", i, sge[i].addr, sge[i].length);
	}
	
	TEST_NZ(ibv_post_send(qpmgt->qp[qp_id], &wr, &bad_wr));
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

int get_wc( struct ibv_wc *wc )
{
	struct ibv_cq *cq;
	void *ctx;
	TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
	ibv_ack_cq_events(cq, 1);
	TEST_NZ(ibv_req_notify_cq(cq, 0));
	int ret = ibv_poll_cq(cq, 1, wc);
	if( ret <= 0 || wc->status != IBV_WC_SUCCESS ){
		printf("get CQE fail: %d wr_id: %d\n", wc->status, (int)wc->wr_id);
		return -1;
	}
	printf("get CQE ok: wr_id: %d type: ", (int)wc->wr_id);
	if( wc->opcode == IBV_WC_SEND ) printf("IBV_WC_SEND\n");
	if( wc->opcode == IBV_WC_RECV ) printf("IBV_WC_RECV\n");
	if( wc->opcode == IBV_WC_RDMA_WRITE ) printf("IBV_WC_RDMA_WRITE\n");
	if( wc->opcode == IBV_WC_RDMA_READ ) printf("IBV_WC_RDMA_READ\n");
	return 0;
}

int qp_query( int qp_id )
{
	struct ibv_qp_attr attr;
	enum ibv_qp_attr_mask attr_mask;
	struct ibv_qp_init_attr init_attr;
	struct ibv_qp *qp = qpmgt->qp[qp_id];
	if( qpmgt->qp_state[qp_id] == 1 ){
		printf("qp id: %d state: -1\n", qp_id);
		return -1;
	}
	
	attr_mask |= IBV_QP_STATE;
	attr_mask |= IBV_QP_CUR_STATE;
	ibv_query_qp( qp, &attr, attr_mask, &init_attr );
	printf("qp id: %d state: %d\n", qp_id, attr.qp_state);
	if( attr.qp_state != 3 ){
		qpmgt->qp_state[qp_id] = 1;
		qpmgt->wrong_number ++;
		if( qpmgt->wrong_number >= qpmgt->number ){
			fprintf(stderr, "All qps die, programme stopped\n");
			exit(1);
		}
	}
	return attr.qp_state;
}

/*
-1 no free query interval [offset, offset+size)
offset is of the bit array, not the original one
*/
int query_bit_free( uint *bit, int offset, int size )
{
	int j;
	offset /= 32; size /= 32;
	size += offset;
	for( int i = offset; i < size; i ++ ){
		if( bit[i] == (~0) ) continue;
		j = 0;
		for( j = 0; j < 32; j ++ ){
			if( !( (1<<j) & bit[i] ) ){
				bit[i] |= (1<<j);
				return i*32+j;
			}
		}
	}
	return -1;
}

int cmp( const void *a, const void *b )
{
	return *(int *)a > *(int *)b ? 1 : -1;
}

/*
cnt 代表未完成擦除的数量，0为完全成功
*/
int update_bit( uint *bit, int offset, int size, int *data, int len )
{
	int i, j;
	int cnt = 0;
	offset /= 32; size /= 32;
	for( i = 0; i < len; i ++ ){
		if( offset*32 <= data[i] && data[i] < (offset+size)*32 ){
			bit[ data[i]/32 ] ^= ( 1 << (data[i]%32) );
		}
		else cnt ++;
	}
	return cnt;
}