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
 one_rq_start, one_rq_end, sum_tran;
extern double ib_send_time;
 
 
int dt[300005], d_count = 0, l_count = 0;
double rq_latency[300005];
int rq_latency_sum[1005];//1us

int main(int argc, char **argv)
{
	int i, j;
	base = elapse_sec();
	
	mpl = (struct memory_pool *)malloc(sizeof(struct memory_pool));
	mpl->pool = (char *)malloc(request_size);
	
	initialize_active( mpl->pool, request_size, argv[1], argv[2] );
	
	struct ibv_wc wc;
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge[10];
	double send_timeout = 0.0, recv_timeout = 0.0, tmp_time;
	for( i = 0; i < 10000; i ++ ){
		tmp_time = elapse_sec();
		memset(&wr, 0, sizeof(wr));
		wr.wr_id = (uintptr_t)0;
		wr.opcode = IBV_WR_RDMA_WRITE;
		wr.send_flags = IBV_SEND_SIGNALED;
		wr.wr.rdma.remote_addr = (uintptr_t)memgt->peer_mr.addr;
		wr.wr.rdma.rkey = memgt->peer_mr.rkey;
		
		wr.sg_list = sge;
		wr.num_sge = 1;
		sge[0].addr = mpl->pool;
		sge[0].length = request_size;
		sge[0].lkey = memgt->rdma_send_mr->lkey;
		
		TEST_NZ(ibv_post_send(qpmgt->qp[0], &wr, &bad_wr));
		send_timeout += elapse_sec()-tmp_time;
		tmp_time = elapse_sec();
		
		get_wc(&wc);
		recv_timeout += elapse_sec()-tmp_time;
		//if( i%100 == 0 ) printf("%d work completed\n", i);
	}
	
	printf("send_timeout %lf recv_timeout %lf\n", send_timeout/10000, recv_timeout/10000);

}