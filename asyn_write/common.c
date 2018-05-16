#include "common.h"

struct connection *s_ctx;
struct memory_management *memgt;
struct qp_management *qpmgt;
struct rdma_cm_event *event;
struct rdma_event_channel *ec;
struct rdma_cm_id *conn_id[128], *listener[128];
int end;//active 0 backup 1

int bind_port = 45679;
int BUFFER_SIZE = 20*1024*1024;
int RDMA_BUFFER_SIZE = 1024*1024*64;
int thread_number = 1;
int connect_number = 2;
int ctrl_number = 1;
int cq_ctrl_num = 1;
int cq_data_num = 1;
int cq_size = 4096;
int qp_size = 4096;
int qp_size_limit = 3000;
double qp_rate = 0.5;

int buffer_per_size;
int recv_buffer_num = 200;
int package_pool_size = 8000;
int full_time_interval = 1000;//us 超时重传时间间隔
int test_time = 3;
int waiting_time = 0;//us 等待可用bitmap时间

int resend_limit = 3;
int request_size = 8*1024;//B
int scatter_size = 1;
int package_size = 1;
int work_timeout = 0;//us 等待可用qp队列时间     
int recv_imm_data_num = 200;
int request_buffer_size = 300000;
int scatter_buffer_size = 64;
int task_pool_size = 16000;
int scatter_pool_size = 8000;

int ScatterList_pool_size = 16000;
int request_pool_size = 16000;

int bit_map[256];

extern double query;
double ib_send_time = 0.0;
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

	qp_attr->cap.max_send_wr = qp_size;
	qp_attr->cap.max_recv_wr = qp_size;
	qp_attr->cap.max_send_sge = 20;
	qp_attr->cap.max_recv_sge = 20;
	qp_attr->cap.max_inline_data = 200;
	
	qp_attr->sq_sig_all = 1;
	
	TEST_NZ(rdma_create_qp(id, s_ctx->pd, qp_attr));
	qpmgt->qp[tid] = id->qp;
	qpmgt->qp_state[tid] = 0;
	qpmgt->qp_count[tid] = 0;
	
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
	
	wr.sg_list = sge;
	wr.num_sge = sct->number;
	
	for( int i = 0; i < sct->number; i ++ ){
		sge[i].addr = (uintptr_t)sct->task[i]->request->sl->address;
		sge[i].length = sct->task[i]->request->sl->length;
		sge[i].lkey = memgt->rdma_send_mr->lkey;
		sct->task[i]->request->tran = elapse_sec();
	}
	
	qpmgt->qp_count[qp_id] ++;
	
	double tmp_time = elapse_sec();
	TEST_NZ(ibv_post_send(qpmgt->qp[qp_id], &wr, &bad_wr));
	ib_send_time += elapse_sec()-tmp_time;
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
	//printf("get CQE ok: wr_id: %d\n", (int)wc->wr_id);
	// if( wc->opcode == IBV_WC_SEND ) printf("IBV_WC_SEND\n");
	// if( wc->opcode == IBV_WC_RECV ) printf("IBV_WC_RECV\n");
	// if( wc->opcode == IBV_WC_RDMA_WRITE ) printf("IBV_WC_RDMA_WRITE\n");
	// if( wc->opcode == IBV_WC_RDMA_READ ) printf("IBV_WC_RDMA_READ\n");
#ifdef _TEST_SYN
	return wc->wr_id;
#else
	return 0;
#endif
}

int qp_query( int qp_id )
{
	if( qpmgt->qp_state[qp_id] == 1 || qpmgt->qp_count[qp_id] >= qp_size_limit ){
		//printf("qp id: %d state: -1\n", qp_id);
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
		fprintf(stderr, "qp: %d num %d\n", i,qpmgt->qp_count[i]);
		rdma_destroy_qp(conn_id[i]);
		rdma_destroy_id(conn_id[i]);
		//fprintf(stderr, "rdma #%02d disconnect\n", i);
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
	
	if( end == 0 ){
		final_bitmap(memgt->send);
		for( int i = 0; i < thread_number; i ++ )
			final_bitmap(memgt->peer[i]);
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

uchar lowbit( uchar x )
{
	return x&(x^(x-1));
}

int init_bitmap( struct bitmap **btmp, int size )
{
	(*btmp) = (struct bitmap *)malloc(sizeof(struct bitmap));
	(*btmp)->bit = (uchar *)malloc((size+7)/8);
	memset( (*btmp)->bit, 0, sizeof((*btmp)->bit) );
	(*btmp)->size = size;
	(*btmp)->handle = 0;
	(*btmp)->test = 0.0;
	TEST_NZ(pthread_mutex_init(&(*btmp)->mutex, NULL));
	TEST_NZ(pthread_spin_init(&(*btmp)->spin, NULL));
	for( int i = 0; i < 8; i ++ ) bit_map[1<<i] = i;
	return 0;
}

int final_bitmap( struct bitmap *btmp )
{
	free(btmp->bit);
	TEST_NZ(pthread_mutex_destroy(&btmp->mutex));
	TEST_NZ(pthread_spin_destroy(&btmp->spin));
	free(btmp);
	return 1;
}

int query_bitmap( struct bitmap *btmp )
{
	int i, ret = -1;
	uchar c;
	int limit = 2000;
	while(limit){
#ifdef __MUTEX		
		pthread_mutex_lock(&btmp->mutex);
#else
		pthread_spin_lock(&btmp->spin);
#endif
		//double tmp_time = elapse_sec();
		for( i = btmp->handle; i < (btmp->size+7)/8; i ++ ){
			c = lowbit(~(btmp->bit[i]));
			if( c == 0 ) continue;
			else{
				if( bit_map[c]+i*8 >= btmp->size ) continue;
				btmp->bit[i] |= c;
				btmp->handle = i;
				ret = bit_map[c]+i*8;
#ifdef __MUTEX
				pthread_mutex_unlock(&btmp->mutex);
#else
				pthread_spin_unlock(&btmp->spin);
#endif
				//query += elapse_sec()-tmp_time;
				break;
			}
		}
		if( ret != -1 ) break;
		for( i = 0; i < btmp->handle; i ++ ){
			c = lowbit(~(btmp->bit[i]));
			if( c == 0 ) continue;
			else{
				if( bit_map[c]+i*8 >= btmp->size ) continue;
				btmp->bit[i] |= c;
				btmp->handle = i;
				ret = bit_map[c]+i*8;
#ifdef __MUTEX
				pthread_mutex_unlock(&btmp->mutex);
#else
				pthread_spin_unlock(&btmp->spin);
#endif
				//query += elapse_sec()-tmp_time;
				break;
			}
		}
		if( ret != -1 ) break;
#ifdef __MUTEX
		pthread_mutex_unlock(&btmp->mutex);
#else
		pthread_spin_unlock(&btmp->spin);
#endif
		//fprintf(stderr, "no more space waiting...\n");
		//usleep(waiting_time);
		//query += elapse_sec()-tmp_time;
		//limit --;
	}
	if( limit ){
		// if( end == 0 )
			// for( int i = 0; i < thread_number; i ++ )
				// if( btmp == memgt->peer[i] ){
					// printf("use remote %d\n", ret);
					// break;
				// }
		return ret;
	}
	else{
		if( btmp == memgt->send )
			printf("no space in send_buffer\n");
		for( int i = 0; i < thread_number; i ++ ){
			if( btmp == memgt->peer[i] ) printf("no space remote thread #%d\n", i);
		}
		printf("no more space\n");
		exit(1);
	}
	return -1;
}

int update_bitmap( struct bitmap *btmp, int *data, int len )
{
	int i, j;
	int cnt = 0;
#ifdef __MUTEX
	pthread_mutex_lock(&btmp->mutex);
#else
	pthread_spin_lock(&btmp->spin);
#endif
	for( i = 0; i < len; i ++ ){
		if( data[i] >= btmp->size ){
			cnt ++;
			printf("data: %d size %d\n", data[i], btmp->size);
		}
		else{
			btmp->bit[data[i]/8] ^= ( (uchar)1 << data[i]%8 );
		}
	}
#ifdef __MUTEX
	pthread_mutex_unlock(&btmp->mutex);
#else
	pthread_spin_unlock(&btmp->spin);
#endif
	return cnt;
}

int query_bitmap_free( struct bitmap *btmp )
{
	int i, cnt = 0;
	for( i = 0; i < (btmp->size+7)/8; i ++ ){
		for( int j = 0; j < 8; j ++ ){
			if( btmp->bit[i] & (1<<j) )
				cnt ++;
		}
	}
	return cnt;
}

int max( int a, int b )
{
	return (a > b) ? a : b;
}