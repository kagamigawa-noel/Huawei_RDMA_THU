#ifndef RDMA_COMMON_H
#define RDMA_COMMON_H

#define _GNU_SOURCE
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>  
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>  

//#define __DEBUG
#define __OUT
//#define __MUTEX

#ifdef __DEBUG
#define DEBUG(info,...)    printf(info, ##__VA_ARGS__)
#else
#define DEBUG(info,...)
#endif

#ifdef __OUT
#define OUT(info,...)    printf(info, ##__VA_ARGS__)
#else
#define OUT(info,...)
#endif

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define TIMEOUT_IN_MS 500
typedef unsigned int uint;
typedef unsigned long long ull;
typedef unsigned char uchar;

enum type{ READ, WRITE };

struct ScatterList
{
	struct ScatterList *next;
	void *address;
	int length;
};

struct bitmap
{
	int size, handle;
	uchar *bit;
	pthread_mutex_t mutex;
	pthread_spinlock_t spin;
};

struct connection
{
	struct ibv_context *ctx;
	struct ibv_pd *pd;
	struct ibv_cq **rd_cq_data, **wt_cq_data, **rd_cq_ctrl, **wt_cq_ctrl;
	struct ibv_comp_channel *rd_comp_channel, *wt_comp_channel;
};

struct memory_management
{
	int number;
	
	struct ScatterList application;
	
	struct ibv_mr *rdma_send_mr;
	struct ibv_mr *rdma_recv_mr;
	
	struct ibv_mr *send_mr;
	struct ibv_mr *recv_mr;
	
	struct ibv_mr peer_mr;
	
	char *recv_buffer;
	char *send_buffer;
	
	char *rdma_send_region;
	char *rdma_recv_region;
	
	struct bitmap *send[4], *peer[4];
};

struct qp_management
{
	int data_num;
	int ctrl_num;
	int data_wrong_num;
	int ctrl_wrong_num;
	struct ibv_qp *qp[128];
	int qp_state[128];
	int qp_count[128];
};

// request <=> task

struct task_active;
struct task_backup;
struct request_active;

struct request_active
{
	void *private;
	struct ScatterList *sl;
	struct task_active *task;
	void (*callback)(struct request_active *);
	double st, get, tran, back, mete_back, ed, into;
};

struct task_active
{
	struct request_active *request;
	struct ScatterList remote_sge;
	short state;
	uint belong;
	int resend_count, qp_id, send_id;
	/*
	-1 failure while transfer
	0 request arrive
	1 commit RNIC to rdma write
	2 recv write CQE
	3 notify remote to commit
	4 recv commit ack
	*/
};

/***************************************/

/*
active:

rdma_write(scatter) information
wr_id: scatter_pointer (64bit)
imm_data: no

send(package) information
wr_id: package_pointer (64bit)
imm_data: no

recv(package) information
wr_id: qp_id
imm_data: package id in pool ( from remote )

backup:

send(package) information
wr_id: 0
imm_data: package id in pool

recv(package) information
wr_id: qp_id
imm_data: no

包： package_active pool下标+total number+
{request->private + ScatterList}

size: 4+4+20*package_size
*/

struct request_backup
{
	void *private;
	struct ScatterList *sl;
	struct task_backup *task;
};

struct task_backup
{
	struct request_backup *request;
	struct ScatterList remote_sge, local_sge;
	short state;
	int resend_count, belong;
	uint task_active_id;
	enum type tp;
};

extern struct connection *s_ctx;
extern struct memory_management *rd_memgt, *wt_memgt;
extern struct qp_management *rd_qpmgt, *wt_qpmgt;
extern struct rdma_cm_event *event;
extern struct rdma_event_channel *ec;
extern struct rdma_cm_id *conn_id[64], *listener[64];
extern int end;//active 0 backup 1
/* both */
extern int BUFFER_SIZE;
extern int RDMA_BUFFER_SIZE;
extern int thread_number;
extern int connect_number;
extern int buffer_per_size;
extern int ctrl_number;
extern int test_time;
extern int recv_buffer_num;//主从两端每个qp控制数据缓冲区个数
extern int cq_ctrl_num;
extern int cq_data_num;
extern int qp_size;
extern int cq_size;
extern int qp_size_limit;
extern int task_pool_size;
/* active */
extern int resend_limit;
extern int request_size;
extern int metedata_size;
extern int work_timeout;
extern int recv_imm_data_num;//主端接收从端imm_data wr个数
extern int request_buffer_size;
/* backup */
extern int ScatterList_pool_size;
extern int request_pool_size;

int on_connect_request(struct rdma_cm_id *id, int tid);
int on_connection(struct rdma_cm_id *id, int tid);
int on_addr_resolved(struct rdma_cm_id *id, int tid);
int on_route_resolved(struct rdma_cm_id *id, int tid);
void build_connection(struct rdma_cm_id *id, int tid);
void build_context(struct ibv_context *verbs);
void build_params(struct rdma_conn_param *params);
void register_memory( int tid, struct memory_management *memgt );
void post_recv( int qp_id, ull tid, int offset, int recv_size, enum type tp );
void post_send( int qp_id, ull tid, int offset, int send_size, int imm_data, enum type tp );
void post_rdma_write( int qp_id, struct task_active *task, int imm_data );
void post_rdma_read( int qp_id, struct task_backup *task );
void die(const char *reason);
int get_wc( struct ibv_wc *wc, enum type tp );
int qp_query( int qp_id, enum type tp );
int re_qp_query( int qp_id, enum type tp );
int query_bit_free( uint *bit, int offset, int size );
int update_bit( uint *bit, int offset, int size, int *data, int len );
int destroy_qp_management( enum type tp );
int destroy_connection();
int destroy_memory_management( int end, enum type tp );
double elapse_sec();
uchar lowbit( uchar x );
int init_bitmap( struct bitmap **btmp, int size );
int final_bitmap( struct bitmap *btmp );
int query_bitmap( struct bitmap *btmp );
int update_bitmap( struct bitmap *btmp, int *data, int len );

#endif