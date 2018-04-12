#ifndef RDMA_COMMON_H
#define RDMA_COMMON_H

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/types.h>  

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define TIMEOUT_IN_MS 500
#define BUFFER_SIZE 32768
#define RDMA_BUFFER_SIZE 65536
typedef unsigned int uint;
typedef unsigned long long ull;

struct ScatterList
{
	struct ScatterList *next;
	void *address;
	int length;
};

struct connection
{
	struct ibv_context *ctx;
	struct ibv_pd *pd;
	struct ibv_cq *cq_data, *cq_ctrl;
	struct ibv_comp_channel *comp_channel;
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
	
	uint *send_bit;
	uint *recv_bit;
	uint *peer_bit;//[64]
	
	pthread_mutex_t rdma_mutex[4];
};

struct qp_management
{
	int data_num;
	int ctrl_num;
	int data_wrong_num;
	int ctrl_wrong_num;
	struct ibv_qp *qp[128];
	int qp_state[128];
};

// request <=> task < scatter < package

struct task_active;
struct task_backup;

struct request_active
{
	void *private;
	struct ScatterList *sl;
	struct task_active *task;
	void *callback;
};

struct task_active
{
	struct request_active *request;
	struct ScatterList remote_sge;
	short state;
	/*
	0 request arrive
	1 commit RNIC to rdma write
	2 recv write CQE
	3 notify remote to commit
	4 recv commit ack
	*/
};

struct scatter_active
{
	int number;
	int qp_id;
	int resend_count;
	struct task_active *task[10];
	struct ScatterList remote_sge;
	struct package_active *package;
};

struct package_active
{
	int number;
	int send_buffer_id;
	int resend_count;
	struct scatter_active *scatter[10];
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

包： package_active pool下标+packge number+
{scatter number + {request->private + ScatterList} }

size: 4+4+20*package_size
*/

struct request_backup
{
	void *private;
	struct ScatterList *sl;
	struct package_backup *package;
};

struct package_backup
{
	int num_finish, number;
	struct request_backup *request[64];
	uint package_active_id;
};


struct connection *s_ctx;
struct memory_management *memgt;
struct qp_management *qpmgt;
struct rdma_cm_event *event;
struct rdma_event_channel *ec;
struct rdma_cm_id *conn_id[128], *listener[128];
int end;//active 0 backup 1
/* both */
extern int thread_number;
extern int connect_number;
extern int buffer_per_size;
extern int ctrl_number;
/* active */
extern int resend_limit;
extern int request_size;
extern int scatter_size;
extern int package_size;
extern int work_timeout;
/* backup */
extern int recv_buffer_num;

int on_connect_request(struct rdma_cm_id *id, int tid);
int on_connection(struct rdma_cm_id *id, int tid);
int on_addr_resolved(struct rdma_cm_id *id, int tid);
int on_route_resolved(struct rdma_cm_id *id, int tid);
void build_connection(struct rdma_cm_id *id, int tid);
void build_context(struct ibv_context *verbs);
void build_params(struct rdma_conn_param *params);
void register_memory( int tid );
void post_recv( int qp_id, ull tid, int offset );
void post_send( int qp_id, ull tid, void *start, int send_size, int imm_data );
void post_rdma_write( int qp_id, struct scatter_active *sct );
void send_package( struct package_active *now, int ps, int offset, int qp_id  );
void die(const char *reason);
int get_wc( struct ibv_wc *wc );
int qp_query( int qp_id );
int query_bit_free( uint *bit, int offset, int size );
int update_bit( uint *bit, int offset, int size, int *data, int len );

#endif