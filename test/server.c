#include "rdma_common.h"

static int on_connect_request(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);
static void send_message(struct connection *conn);

static void send_mr(void * context);

static void on_completion(struct ibv_wc *wc);

static void post_receives(struct connection *conn);
static void post_receives_msg(struct connection *conn);
static void die(const char *reason);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void * poll_cq(void *);
static void register_memory(struct connection *conn);

static struct context *s_ctx = NULL;

int main(int argc, char **argv)
{
  struct sockaddr_in addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *listener = NULL;
  struct rdma_event_channel *ec = NULL;
  uint16_t port = 0;

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;

  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &listener, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_bind_addr(listener, (struct sockaddr *)&addr));
  TEST_NZ(rdma_listen(listener, 10)); /* backlog=10 is arbitrary */

  port = ntohs(rdma_get_src_port(listener));

  printf("listening on port %d.\n", port);

  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }

  rdma_destroy_id(listener);
  rdma_destroy_event_channel(ec);

  return 0;
}

int on_connect_request(struct rdma_cm_id *id)
{
  TIPS(on_connect_request);

  struct connection *conn;
  struct ibv_qp_init_attr qp_attr;
  struct rdma_conn_param cm_params;

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id,s_ctx->pd,&qp_attr));

  id->context = conn = (struct connection *) malloc(sizeof(struct connection));

  conn->id = id;
  conn->qp = id->qp;
  conn->send_state = SS_INIT;
  conn->recv_state = RS_INIT;

  conn->connected = 1;

  register_memory(conn);
  post_receives(conn);

  memset(&cm_params, 0, sizeof(cm_params));

  cm_params.initiator_depth = cm_params.responder_resources = 1;
  cm_params.rnr_retry_count = 7;

  printf("received connection request.\n");
  TEST_NZ(rdma_accept(id, &cm_params));     

  return 0;
}

int on_connection(void *context)
{
  struct connection *conn = (struct connection *)context;

  snprintf(conn->rdma_local_region, BUFFER_SIZE, "message from passive/server side with pid %d", getpid());


  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  TIPS(on_disconnect);
  struct connection *conn = (struct connection *)id->context;

  printf("peer disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->rdma_remote_mr);
  ibv_dereg_mr(conn->rdma_local_mr);

  free(conn->rdma_local_region);
  free(conn->rdma_remote_region);

  free(conn);

  rdma_destroy_id(id);

  return 0;
}

int on_event(struct rdma_cm_event *event)
{
  TIPS(on_event);
  int r = 0;

  if (event->event == RDMA_CM_EVENT_CONNECT_REQUEST)   //由远端rdma_connect触发
    r = on_connect_request(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)   //由本地rdma_accept触发
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)   //由远端rdma_disconnect
    r = on_disconnect(event->id);
  else
    die("on_event: unknown event.");

  return r;
}

void send_message(struct connection *conn)
{
  struct ibv_send_wr wr,*bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr,0,sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->send_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->send_msg_mr->lkey;

  while(!conn->connected);

  TEST_NZ(ibv_post_send(conn->qp,&wr,&bad_wr));
  
}

void send_mr(void * context)
{
  struct connection * conn = (struct connection *) context;

  conn->send_msg->type = MSG_MR;
  memcpy(&conn->send_msg->data.mr,conn->rdma_local_mr,sizeof(struct ibv_mr));     //将remote_mr发送到远端

  send_message(conn);
  conn->send_msg->type = MSG_DONE;
}

 void on_completion(struct ibv_wc *wc)
{
  TIPS(on_completion);
  struct connection *conn = (struct connection*)(uintptr_t)wc->wr_id;

  if (wc->status != IBV_WC_SUCCESS) {
      fprintf(stderr, "Send Completion reported failure: %s (%d)\n",
              ibv_wc_status_str(wc->status), wc->status);
      die("on_completion: status is not IBV_WC_SUCCESS.");
  }
  if(wc->opcode & IBV_WC_RECV)
  {
    if(conn->recv_state == RS_INIT)
    {
      printf("recv first  send mr+post recv\n");
      send_mr(conn);
      post_receives(conn);
    }
    if(conn->recv_state == RS_MR_RECV){
      printf("send MSG_DONE\n");
      conn->send_msg->type = MSG_DONE;
      send_message(conn);
      post_receives_msg(conn);
    }
  }
  if(conn->recv_msg->type == MSG_DONE){
    printf("remote buffer: %s\n",conn->rdma_local_region);
    rdma_disconnect(conn->id);
  }
  if(wc->opcode & IBV_WC_RECV){
    conn->recv_state++;
    printf("recv completed successfully.\n");
  }
  else{
    conn->send_state++;
    printf("send completed successfully.\n");
  }
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}



void post_receives(struct connection *conn)
{
  TIPS(post_receives);
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->rdma_remote_region;
  sge.length = BUFFER_SIZE;
  sge.lkey = conn->rdma_remote_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void post_receives_msg(struct connection *conn)
{
  TIPS(post_receives);
  struct ibv_recv_wr wr, *bad_wr = NULL;
  struct ibv_sge sge;

  wr.wr_id = (uintptr_t)conn;
  wr.next = NULL;
  wr.sg_list = &sge;
  wr.num_sge = 1;

  sge.addr = (uintptr_t)conn->recv_msg;
  sge.length = sizeof(struct message);
  sge.lkey = conn->recv_msg_mr->lkey;

  TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void build_context(struct ibv_context *verbs)
{
  TIPS(build_context);
  if (s_ctx) {
    if (s_ctx->ctx != verbs)
      die("cannot handle events in more than one context.");

    return;
  }

  s_ctx = (struct context *)malloc(sizeof(struct context));

  s_ctx->ctx = verbs;

  TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  TEST_Z(s_ctx->comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  TEST_Z(s_ctx->cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->comp_channel, 0)); /* cqe=10 is arbitrary */
  TEST_NZ(ibv_req_notify_cq(s_ctx->cq, 0));

  TEST_NZ(pthread_create(&s_ctx->cq_poller_thread, NULL, poll_cq, NULL));  //开启线程轮询cq
}

 void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  TIPS(build_qp_attr);
  memset(qp_attr, 0, sizeof(*qp_attr));

  qp_attr->send_cq = s_ctx->cq;
  qp_attr->recv_cq = s_ctx->cq;
  qp_attr->qp_type = IBV_QPT_RC;

  qp_attr->cap.max_send_wr = 10;
  qp_attr->cap.max_recv_wr = 10;
  qp_attr->cap.max_send_sge = 1;
  qp_attr->cap.max_recv_sge = 1;
}

 void * poll_cq(void *ctx)
{
  TIPS(poll_cq);
  struct ibv_cq *cq;
  struct ibv_wc wc;

  while (1) {
    TEST_NZ(ibv_get_cq_event(s_ctx->comp_channel, &cq, &ctx));
    ibv_ack_cq_events(cq, 1);
    TEST_NZ(ibv_req_notify_cq(cq, 0));

    while (ibv_poll_cq(cq, 1, &wc))
      on_completion(&wc);
  }

  return NULL;
}


 void register_memory(struct connection *conn)
{
  TIPS(register_memory);
  conn->rdma_local_region = malloc(BUFFER_SIZE);
  conn->rdma_remote_region = malloc(BUFFER_SIZE);

  conn->send_msg = malloc(sizeof(struct message));
  conn->recv_msg = malloc(sizeof(struct message));



  TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_local_region, 
    BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_remote_region, 
    BUFFER_SIZE, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->send_msg_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->send_msg, 
    sizeof(struct message), 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->recv_msg_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->recv_msg, 
    sizeof(struct message), 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
}
