#include "common.h"

int app_buffer_size = 100;

struct request_pool
{
	struct request_active *pool;
	double *latency;
	int *queue;
	int count, front, tail;
	pthread_mutex_t rpl_mutex;
};

struct memory_pool
{
	char *pool;
	int *queue;
	int count, front, tail;
	pthread_mutex_t mpl_mutex;
};

struct ScatterList_pool
{
	struct ScatterList *pool;
	int *queue;
	int count, front, tail;
	pthread_mutex_t SLpl_mutex;
};

extern int write_count, request_count, back_count, send_package_count;
struct request_pool *rpl;
struct memory_pool *mpl;
struct ScatterList_pool *SLpl;
double rq_back, rq_start, rq_end, base, get_working, do_working,\
 cq_send, cq_recv, cq_write, cq_waiting, cq_poll,\
 q_task, other, query, send_package_time, end_time,\
 working_write, q_qp, init_remote, init_scatter, q_scatter,\
 one_rq_start, one_rq_end, sum_tran, sbf_time, call_time, callback_time;
extern double ib_send_time;
 
int cmp( const void *a, const void *b )
{
	return *(int *)a > *(int *)b ? 1 : -1;
}
 
int dt[300005], d_count = 0, l_count = 0, rq_sub;
double rq_latency[300005];
int rq_latency_sum[1005];//1us

int main(int argc, char **argv)
{
	if( argc != 4 ){
		printf("error input\n");
		exit(1);
	}
	rq_sub = atoi(argv[2]);
	int i, j;
	base = elapse_sec();
	
	mpl = (struct memory_pool *)malloc(sizeof(struct memory_pool));
	mpl->pool = (char *)malloc(request_size*rq_sub);
	
	initialize_active( mpl->pool, request_size*rq_sub, argv[1], argv[3] );
	
	struct ibv_wc wc[100];
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge[10];
	double send_timeout = 0.0, recv_timeout = 0.0, tmp_time;
	int scat = rq_sub;
	printf("test start\n");
	double ab = elapse_sec();
	for( i = 0; i < rq_sub; i += scat ){
		double t1[5000];
		for( j = 0; j < scat; j ++ ){
			//tmp_time = elapse_sec();
			memset(&wr, 0, sizeof(wr));
			wr.wr_id = (uintptr_t)i+j;
			wr.opcode = IBV_WR_RDMA_WRITE;
			wr.send_flags = IBV_SEND_SIGNALED;
			wr.wr.rdma.remote_addr = (uintptr_t)memgt->peer_mr.addr+request_size*j;
			wr.wr.rdma.rkey = memgt->peer_mr.rkey;
			
			wr.sg_list = sge;
			wr.num_sge = 1;
			sge[0].addr = mpl->pool+request_size*j;
			sge[0].length = request_size;
			sge[0].lkey = memgt->rdma_send_mr->lkey;
			
			TEST_NZ(ibv_post_send(qpmgt->qp[0], &wr, &bad_wr));
			send_timeout += elapse_sec()-tmp_time;
			tmp_time = elapse_sec();
			printf("%d submit ok\n", i+j);
		}
		
		void *ctx;
		struct ibv_cq *cq;
		int sum = 0;
		int a[5005], a_cnt = 0;
		while(1){
			TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
			ibv_ack_cq_events(cq, 1);
			TEST_NZ(ibv_req_notify_cq(cq, 0));
			while(1){
				int ret = ibv_poll_cq(cq, 100, wc);
				if( ret <= 0 ){
					//printf("get CQE fail: %d wr_id: %d\n", wc->status, (int)wc->wr_id);
					break;
				}
				else sum += ret;
				for( j = 0; j < ret; j ++ ){
					if( wc[j].status != IBV_WC_SUCCESS ){
						printf("error %d status %d\n", wc[j].wr_id, wc[j].status);
						continue;
					}
					a[a_cnt++] = wc[j].wr_id;
				}
			}
			if( sum == scat ) break;
		}
		
		qsort( a, scat, sizeof(int), cmp );
		for( j = 0; j < scat; j ++ ){
			if( i+j != a[j] )
				printf("wrong %d %d\n", i+j, a[j]);
		}
		puts("");
		recv_timeout += elapse_sec()-tmp_time;
		//if( i%100 == 0 ) printf("%d work completed\n", i);
	}
	send_timeout = elapse_sec()-ab;
	printf("test end\n");
	printf("send_timeout %lf recv_timeout %lf\n", send_timeout/1000.0, recv_timeout/10000);

}