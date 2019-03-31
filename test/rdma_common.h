#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)
#define TIPS(x) do { printf("--" #x " \n"); } while (0)\

const int BUFFER_SIZE = 1024;

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *cq;
  struct ibv_comp_channel *comp_channel;

  pthread_t cq_poller_thread;
};

struct message {
	enum {
		MSG_WR,
		MSG_MR,
		MSG_DONE
	} type ;
	union{ 
		struct ibv_mr mr ;
	} data ;
};

struct connection
{
	struct rdma_cm_id *id ;
	struct ibv_qp *qp ;
	
	int connected ; 
	
	struct ibv_mr *recv_msg_mr ; 
	struct ibv_mr *send_msg_mr ; 
	struct ibv_mr *rdma_local_mr ; 
	struct ibv_mr *rdma_remote_mr ; 
	
	struct ibv_mr peer_mr ;
	
	struct message * recv_msg ;
	struct message * send_msg;

	char * rdma_local_region;
	char * rdma_remote_region;

	enum{
		SS_INIT,
		SS_MR_SENT,
		SS_RDMA_SENT,
		SS_DONE_SENT,
	} send_state;

	enum{
		RS_INIT,
		RS_MR_RECV,
		RS_DONE_RECV,
	} recv_state;

	enum{
		M_NONE,
		M_WRITE,
		M_READ,
	}mode;
};

//extern void build_connection(struct rdma_cm_id *id);

//extern void bulid_params(struct rdma_conn_param *params);

// extern void send_message(struct connection *conn);

// extern void send_mr(void * context);

// extern void on_completion(struct ibv_wc *wc);

// extern void post_receives(struct connection *conn);

// extern void die(const char *reason);

// extern void build_context(struct ibv_context *verbs);
// extern void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
// extern void * poll_cq(void *);
// extern void register_memory(struct connection *conn);
