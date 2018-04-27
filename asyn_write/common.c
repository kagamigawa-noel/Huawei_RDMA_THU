#include "common.h"

int BUFFER_SIZE = 64*1024*1024;
int RDMA_BUFFER_SIZE = 1024*1024*64;
int thread_number = 4;
int connect_number = 12;//num of qp used to transfer data shouldn't exceed 12
int buffer_per_size;
int ctrl_number = 4;
int full_time_interval = 100000;// 100ms
int test_time = 3;
int recv_buffer_num = 1000;
int package_pool_size = 8192;
int cq_ctrl_num = 4;
int cq_data_num = 8;
int cq_size = 4096;

int resend_limit = 3;
int request_size = 4*1024;//B
int scatter_size = 4;
int package_size = 4;
int work_timeout = 0;      
int recv_imm_data_num = 400;
int request_buffer_size = 32768;
int scatter_buffer_size = 64;
int task_pool_size = 32768*2;
int scatter_pool_size = 32768;

int ScatterList_pool_size = 32768;
int request_pool_size = 32768;

/*
BUFFER_SIZE >= recv_buffer_num*buffer_per_size*ctrl_number
task: 8192/thread_number
scatter: 8192/scatter_size/thread_number
remote area: RDMA_BUFFER_SIZE/request_size/scatter_size/thread_number
package: 8192
send buffer: BUFFER_SIZE/buffer_per_size
*/

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
	  qpmgt->data_num = connect_number-ctrl_number;
	  qpmgt->ctrl_num = ctrl_number;
	  qpmgt->data_wrong_num = 0;
	  qpmgt->ctrl_wrong_num = 0;
	  //sth need to init for 1st time
	}
	memset(qp_attr, 0, sizeof(*qp_attr));
	
	qp_attr->qp_type = IBV_QPT_RC;
	
	if( tid < qpmgt->data_num ){
		qp_attr->send_cq = s_ctx->cq_data[tid%cq_data_num];
		qp_attr->recv_cq = s_ctx->cq_data[tid%cq_data_num];
	}
	else{
		qp_attr->send_cq = s_ctx->cq_ctrl[tid%cq_ctrl_num];
		qp_attr->recv_cq = s_ctx->cq_ctrl[tid%cq_ctrl_num];
	}

	qp_attr->cap.max_send_wr = 10000;
	qp_attr->cap.max_recv_wr = 10000;
	qp_attr->cap.max_send_sge = 20;
	qp_attr->cap.max_recv_sge = 20;
	qp_attr->cap.max_inline_data = 200;
	
	qp_attr->sq_sig_all = 1;
	
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
	/* pay attention to size of CQ */
	s_ctx->cq_data = (struct ibv_cq **)malloc(sizeof(struct ibv_cq *)*cq_data_num);
	s_ctx->cq_ctrl = (struct ibv_cq **)malloc(sizeof(struct ibv_cq *)*cq_ctrl_num);
	for( int i = 0; i < cq_data_num; i ++ ){
		TEST_Z(s_ctx->cq_data[i] = ibv_create_cq(s_ctx->ctx, cq_size, NULL, s_ctx->comp_channel, 0)); 
		TEST_NZ(ibv_req_notify_cq(s_ctx->cq_data[i], 0));
	}
	for( int i = 0; i < cq_ctrl_num; i ++ ){
		TEST_Z(s_ctx->cq_ctrl[i] = ibv_create_cq(s_ctx->ctx, cq_size, NULL, s_ctx->comp_channel, 0)); 
		TEST_NZ(ibv_req_notify_cq(s_ctx->cq_ctrl[i], 0));
	}
	
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
	
	buffer_per_size = 4+4+(sizeof(void *)+sizeof(struct ScatterList))*scatter_size*package_size;
	
	if( tid == 1 ){//active don't need recv
		memgt->rdma_recv_region = (char *)malloc(RDMA_BUFFER_SIZE);
		TEST_Z( memgt->rdma_recv_mr = ibv_reg_mr( s_ctx->pd, memgt->rdma_recv_region,
		RDMA_BUFFER_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE ) );
	}
	else{
		TEST_Z( memgt->rdma_send_mr = ibv_reg_mr( s_ctx->pd, memgt->application.address,
		memgt->application.length, IBV_ACCESS_LOCAL_WRITE ) );
	}
	
	memgt->send_bit = (uint *)malloc(RDMA_BUFFER_SIZE/request_size/scatter_size/32*4);//*4 can not ignore
	memgt->recv_bit = (uint *)malloc(RDMA_BUFFER_SIZE/request_size/scatter_size/32*4);
	memgt->peer_bit = (uint *)malloc(RDMA_BUFFER_SIZE/request_size/scatter_size/32*4);
	memset( memgt->send_bit, 0, sizeof(RDMA_BUFFER_SIZE/request_size/scatter_size/32*4) );
	memset( memgt->recv_bit, 0, sizeof(RDMA_BUFFER_SIZE/request_size/scatter_size/32*4) );
	memset( memgt->peer_bit, 0, sizeof(RDMA_BUFFER_SIZE/request_size/scatter_size/32*4) );
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
	sge.length = buffer_per_size;
	sge.lkey = memgt->recv_mr->lkey;
	
	TEST_NZ(ibv_post_recv(qpmgt->qp[qp_id], &wr, &bad_wr));
}

void post_send( int qp_id, ull tid, int offset, int send_size, int imm_data )
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
	
	sge.addr = (uintptr_t)memgt->send_buffer+offset;
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
	//printf("rdma write ok\n");
}

void send_package( struct package_active *now, int ps, int offset, int qp_id  )
{
	void *ack_content = memgt->send_buffer+offset;
	void *send_start = ack_content;
	int pos = ps, num = 0;
	for( int i = 0; i < now->number; i ++ ){
		num += now->scatter[i]->number;
	}
	/* copy package_active pool id */
	memcpy( ack_content, &pos, sizeof(pos) );
	ack_content += sizeof(pos);
	
	/* copy package number */
	memcpy( ack_content, &num, sizeof(num) );
	ack_content += sizeof(num);
	
	/* copy scatter */
	for( int i = 0; i < now->number; i ++ ){
		for( int j = 0; j < now->scatter[i]->number; j ++ ){
			/* copy request private */
			memcpy( ack_content, &now->scatter[i]->task[j]->request->private,\
			sizeof( now->scatter[i]->task[j]->request->private ) );
			ack_content += sizeof( now->scatter[i]->task[j]->request->private );
			
			memcpy( ack_content, &now->scatter[i]->task[j]->remote_sge,\
			sizeof(now->scatter[i]->task[j]->remote_sge) );
			ack_content += sizeof(now->scatter[i]->task[j]->remote_sge);
		}
	}
	
	post_send( qp_id, now, \
	offset, ack_content-send_start, 0 );
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}

int get_wc( struct ibv_wc *wc )
{
	void *ctx;
	struct ibv_cq *cq;
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
	if( qpmgt->qp_state[qp_id] == 1 ){
		printf("qp id: %d state: -1\n", qp_id);
		return -1;
	}
	return 3;
}

int re_qp_query( int qp_id )
{
	struct ibv_qp_attr attr;
	struct ibv_qp_init_attr init_attr;
	struct ibv_qp *qp = qpmgt->qp[qp_id];
	if( qpmgt->qp_state[qp_id] == 1 ){
		printf("qp id: %d state: -1\n", qp_id);
		return -1;
	}
	TEST_NZ(ibv_query_qp( qpmgt->qp[qp_id], &attr, IBV_QP_STATE, &init_attr ));
	//attr.qp_state = 3;
	//printf("qp id: %d state: %d\n", qp_id, attr.qp_state);
	if( attr.qp_state != 3 ){
		qpmgt->qp_state[qp_id] = 1;
		printf("qp id: %d state: %d\n", qp_id, attr.qp_state);
		if( qp_id < qpmgt->data_num ){
			qpmgt->data_wrong_num ++;
			if( qpmgt->data_wrong_num >= qpmgt->data_num ){
				fprintf(stderr, "All data qps die, programme stopped\n");
				exit(1);
			}
		}
		else{
			qpmgt->ctrl_wrong_num ++;
			if( qpmgt->ctrl_wrong_num >= qpmgt->ctrl_num ){
				fprintf(stderr, "All ctrl qps die, programme stopped\n");
				exit(1);
			}
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
	int j, up;
	size += offset;
	for( int i = offset/32; i <= (offset+size+31-1)/32; i ++ ){//前面下取整，后面上取整
		if( bit[i] == (~0) ) continue;
		if( i == offset/32 ) j = offset%32;
		else j = 0;
		if( i == (offset+size+31-1)/32 ) up = (offset+size-1)%32;
		else up = 31;
		j = 0;
		for( j = 0; j <= up; j ++ ){
			if( !( (1<<j) & bit[i] ) ){
				bit[i] |= (1<<j);
				return i*32+j;
			}
		}
	}
	return -1;
}

/*
cnt 代表未完成擦除的数量，0为完全成功
*/
int update_bit( uint *bit, int offset, int size, int *data, int len )
{
	int i, j;
	int cnt = 0;
	for( i = 0; i < len; i ++ ){
		if( offset <= data[i] && data[i] < offset+size ){
			bit[ data[i]/32 ] ^= ( 1 << (data[i]%32) );
		}
		else cnt ++;
	}
	return cnt;
}

int destroy_qp_management()
{
	for( int i = 0; i < connect_number; i ++ ){
		//printf("waiting %02d\n", i);
		rdma_disconnect(conn_id[i]);
		//fprintf(stderr, "qp: %d state %d\n", i, qp_query(i));
		rdma_destroy_qp(conn_id[i]);
		rdma_destroy_id(conn_id[i]);
		fprintf(stderr, "rdma #%02d disconnect\n", i);
	}
	free(qpmgt); qpmgt = NULL;
	return 0;
}

int destroy_connection()
{
	for( int i = 0; i < cq_data_num; i ++ )
		TEST_NZ(ibv_destroy_cq(s_ctx->cq_data[i]));
	for( int i = 0; i < cq_ctrl_num; i ++ )
		TEST_NZ(ibv_destroy_cq(s_ctx->cq_ctrl[i]));
	free(s_ctx->cq_data); s_ctx->cq_data = NULL;
	free(s_ctx->cq_ctrl); s_ctx->cq_ctrl = NULL;
	TEST_NZ(ibv_destroy_comp_channel(s_ctx->comp_channel));
	TEST_NZ(ibv_dealloc_pd(s_ctx->pd));
	rdma_destroy_event_channel(ec);
	free(s_ctx); s_ctx = NULL;
	return 0;
}

int destroy_memory_management( int end )// 0 active 1 backup
{	
	TEST_NZ(ibv_dereg_mr(memgt->send_mr));
	free(memgt->send_buffer);  memgt->send_buffer = NULL;
	
	TEST_NZ(ibv_dereg_mr(memgt->recv_mr));
	free(memgt->recv_buffer);  memgt->recv_buffer = NULL;
	
	if( end == 0 ){//active
		TEST_NZ(ibv_dereg_mr(memgt->rdma_send_mr));
	}
	else{//backup
		TEST_NZ(ibv_dereg_mr(memgt->rdma_recv_mr));
		free(memgt->rdma_recv_region); memgt->rdma_recv_region = NULL;
	}
	
	free(memgt->send_bit); memgt->send_bit = NULL;
	free(memgt->recv_bit); memgt->recv_bit = NULL;
	free(memgt->peer_bit); memgt->peer_bit = NULL;
	
	if( end == 0 ){
		for( int i = 0; i < thread_number; i ++ ){
			TEST_NZ(pthread_mutex_destroy(&memgt->rdma_mutex[i]));
		}
	}
	free(memgt); memgt = NULL;
	return 0;
}

double elapse_sec()
{
    struct timeval current_tv;
    gettimeofday(&current_tv,NULL);
    return (double)(current_tv.tv_sec)*1000000.0+\
	(double)(current_tv.tv_usec);
}