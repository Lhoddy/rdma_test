#include "rdma_common.h"
struct timeval start,finish;


const int TIMEOUT_IN_MS = 500; /* ms */

static int on_addr_resolved(struct rdma_cm_id *id);
static int on_route_resolved(struct rdma_cm_id *id);
static int on_connection(void *context);
static int on_disconnect(struct rdma_cm_id *id);
static int on_event(struct rdma_cm_event *event);

static void send_message(struct connection *conn);
static void post_receives_msg(struct connection *conn);

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void conn_register_memory(struct connection *conn);

static void * poll_cq(void *);
static void on_completion(struct ibv_wc *wc);

static void die(const char *reason);
static struct context *s_ctx = NULL;

void time_count(struct timeval start,struct timeval finish)
{
  printf("cost the time is : %ld us.\n",
    1000000 * (finish.tv_sec - start.tv_sec) + finish.tv_usec - start.tv_usec);
}

int main(int argc, char **argv)
{
  
  struct addrinfo *addr;
  struct rdma_cm_event *event = NULL;
  struct rdma_cm_id *conn= NULL;
  struct rdma_event_channel *ec = NULL;

  if (argc != 2)
    die("usage: client <read/write> <server-address> <server-port>");

  TEST_NZ(getaddrinfo("192.168.190.130", argv[1], NULL, &addr));

  TEST_Z(ec = rdma_create_event_channel());
  TEST_NZ(rdma_create_id(ec, &conn, NULL, RDMA_PS_TCP));
  TEST_NZ(rdma_resolve_addr(conn, NULL, addr->ai_addr, TIMEOUT_IN_MS)); //向新产生的ec中加入addr_resolve event

  freeaddrinfo(addr);
  
  
  while (rdma_get_cm_event(ec, &event) == 0) {
    struct rdma_cm_event event_copy;

    memcpy(&event_copy, event, sizeof(*event));
    rdma_ack_cm_event(event);

    if (on_event(&event_copy))
      break;
  }
  
  rdma_destroy_event_channel(ec);

  return 0;
}

int on_addr_resolved(struct rdma_cm_id *id)
{
  TIPS(on_addr_resolved);
  struct connection *conn;
  struct ibv_qp_init_attr qp_attr;

  build_context(id->verbs);
  build_qp_attr(&qp_attr);

  TEST_NZ(rdma_create_qp(id,s_ctx->pd,&qp_attr));

  id->context = conn = (struct connection *) malloc(sizeof(struct connection));

  conn->id = id;
  conn->qp = id->qp;
  conn->send_state = SS_INIT;
  conn->recv_state = RS_INIT;
  conn->mode = M_NONE;
  conn->connected = 1;

  conn_register_memory(conn);
  post_receives_msg(conn);

  TEST_NZ(rdma_resolve_route(id, TIMEOUT_IN_MS));   //发送一个route_resolve的event

  return 0;
}

int on_route_resolved(struct rdma_cm_id *id)
{
  TIPS(on_route_resolved);
  struct rdma_conn_param cm_params;

  memset(&cm_params, 0, sizeof(cm_params));

  cm_params.initiator_depth = cm_params.responder_resources = 1;
  cm_params.rnr_retry_count = 7;

  printf("route resolved.\n");

  TEST_NZ(rdma_connect(id, &cm_params));    //启动连接，1.远端发送的数据立即被接收到;2.在本地放入一个on_connection事件

  return 0;
}


int on_connection(void *context)  //在此之前发送的receive已经被消耗，数据接受完成;做client本次连接要做的事
{
  TIPS(on_connection);
  struct connection *conn = (struct connection *)context;

  snprintf(conn->rdma_local_region, BUFFER_SIZE, "message from active/client side with pid %d", getpid());

  conn->init_conn_type_msg->type = TYPE_W;
  conn->init_conn_type_msg->size = BUFFER_SIZE;
  char filename[80] = "test.txt";
  memcpy(conn->init_conn_type_msg->address,filename,sizeof(char)*80);

  struct ibv_send_wr wr,*bad_wr = NULL;
  struct ibv_sge sge;

  memset(&wr,0,sizeof(wr));

  wr.wr_id = (uintptr_t)conn;
  wr.opcode = IBV_WR_SEND;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.send_flags = IBV_SEND_SIGNALED;

  sge.addr = (uintptr_t)conn->init_conn_type_msg;
  sge.length = sizeof(struct conn_type_message);
  sge.lkey = conn->init_conn_type_msg_mr->lkey;

  TEST_NZ(ibv_post_send(conn->qp,&wr,&bad_wr));
  // conn->send_msg->type = MSG_WR;
  // memcpy(&conn->send_msg->data.mr,conn->rdma_remote_mr,sizeof(struct ibv_mr));     //将remote_mr发送到远端

  // struct ibv_send_wr wr,*bad_wr = NULL;
  // struct ibv_sge sge;

  // memset(&wr,0,sizeof(wr));

  // wr.wr_id = (uintptr_t)conn;
  // wr.opcode = IBV_WR_SEND;
  // wr.sg_list = &sge;
  // wr.num_sge = 1;
  // wr.send_flags = IBV_SEND_SIGNALED;

  // sge.addr = (uintptr_t)conn->rdma_remote_region;
  // sge.length = BUFFER_SIZE;
  // sge.lkey = conn->rdma_remote_mr->lkey;

  // while(!conn->connected);

  // TEST_NZ(ibv_post_send(conn->qp,&wr,&bad_wr));
  
  return 0;
}

int on_disconnect(struct rdma_cm_id *id)
{
  TIPS(on_disconnect);
  struct connection *conn = (struct connection *)id->context;

  printf("disconnected.\n");

  rdma_destroy_qp(id);

  ibv_dereg_mr(conn->rdma_local_mr);
  ibv_dereg_mr(conn->rdma_remote_mr);

  free(conn->rdma_local_region);
  free(conn->rdma_remote_region);

  free(conn);

  rdma_destroy_id(id);

  return 1; /* exit event loop */
}

int on_event(struct rdma_cm_event *event)
{
  TIPS(on_event);
  int r = 0;

  if (event->event == RDMA_CM_EVENT_ADDR_RESOLVED)   //由本地main的rdma_resolve_addr触发
    r = on_addr_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ROUTE_RESOLVED)   //由本地on_addr_resolved的rdma_resolve_route触发
    r = on_route_resolved(event->id);
  else if (event->event == RDMA_CM_EVENT_ESTABLISHED)     //由本地 rdma_connect触发
    r = on_connection(event->id->context);
  else if (event->event == RDMA_CM_EVENT_DISCONNECTED)    //由本地rdma_disconnect触发
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


 void conn_register_memory(struct connection *conn)
{
  TIPS(conn_register_memory);
  conn->rdma_local_region = malloc(BUFFER_SIZE+2);
  conn->rdma_remote_region = malloc(BUFFER_SIZE+2);

  conn->send_msg = malloc(sizeof(struct message));
  conn->recv_msg = malloc(sizeof(struct message));

  conn->init_conn_type_msg = malloc(sizeof(struct conn_type_message));

  TEST_Z(conn->rdma_local_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_local_region, 
    BUFFER_SIZE+2, 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));

  TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->rdma_remote_region, 
    BUFFER_SIZE+2, 
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

  TEST_Z(conn->init_conn_type_msg_mr = ibv_reg_mr(
    s_ctx->pd, 
    conn->init_conn_type_msg, 
    sizeof(struct conn_type_message), 
    IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
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
      //if(s_ctx->type == unset)
        on_completion(&wc);
      //else if(s_ctx->type == write)
        // on_completion_write(&wc);
      //else if(s_ctx->type == read)
        // on_completion_read(&wc);
  }

  return NULL;
}


 void on_completion(struct ibv_wc *wc)
{
  
  struct connection *conn = (struct connection*)(uintptr_t)wc->wr_id;


  if (wc->status != IBV_WC_SUCCESS) {
      fprintf(stderr, "EERROR: %s (%d)\t",
              ibv_wc_status_str(wc->status), wc->status);
      die("on_completion: status is not IBV_WC_SUCCESS.");
  }
  if(wc->opcode & IBV_WC_RECV)
  {
    TIPS(on_completion_RECV);
    conn->recv_state++;
    if(conn->recv_msg->type == MSG_MR)
    {
    	gettimeofday(&start, 0);//计时开始
      memcpy(&conn->peer_mr,&conn->recv_msg->data.mr,sizeof(conn->peer_mr));
      memcpy(&conn->peer_mr2,&conn->recv_msg->data.mr2,sizeof(conn->peer_mr));
      struct ibv_send_wr wr,*bad_wr = NULL;
      struct ibv_sge sge;
      if(conn->mode == M_WRITE)
        printf("received MSG_MR. writing message to remote memory...\n");
      else
        printf("received MSG_MR. reading message from remote memory...\n");
      
      memset(&wr,0,sizeof(wr));
      
      wr.wr_id= (uintptr_t)conn;
      wr.opcode = (conn->mode == M_WRITE) ? IBV_WR_RDMA_WRITE: IBV_WR_RDMA_READ;
      wr.sg_list = &sge;
      wr.num_sge = 1;
      wr.send_flags = IBV_SEND_SIGNALED;
      wr.wr.rdma.remote_addr = (uintptr_t)conn->peer_mr.addr;
      wr.wr.rdma.rkey = conn->peer_mr.rkey;

      sge.addr = (uintptr_t)conn->rdma_local_region;
      sge.length = BUFFER_SIZE;
      sge.lkey = conn->rdma_local_mr->lkey;

      TEST_NZ(ibv_post_send(conn->qp,&wr,&bad_wr));
      
    }
    else if(conn->recv_msg->type == MSG_DONE){
      if(conn->mode == M_WRITE)
        printf("finish!\n");
      else
        printf("remote buffer: %s\n",conn->rdma_local_region);
      rdma_disconnect(conn->id);
    }
  }     
  else
  {
    TIPS(on_completion_SEND);
    if(conn->init_conn_type_msg->type != NONE)
    {
      //for w/r init_conn_type
      if(conn->init_conn_type_msg->type == TYPE_W)
      {
        conn->mode = M_WRITE;
      }
      else
        conn->mode = M_READ;
      TIPS(send_conn_type_msg);
      conn->init_conn_type_msg->type = NONE;
    }
    else if(conn->init_conn_type_msg->type == NONE)
    {
      printf("transfer %d\n",(int) wc->byte_len);
      conn->send_state++;
      if(1)//conn->mode == M_WRITE)
      {
        if( conn->send_state == SS_WRITE_SENT)
        {
          
          struct ibv_send_wr wr,*bad_wr = NULL;
          struct ibv_sge sge;
          memset(&wr,0,sizeof(wr));
          wr.wr_id= (uintptr_t)conn;
          wr.opcode = IBV_WR_RDMA_READ;
          wr.sg_list = &sge;
          wr.num_sge = 1;
          wr.send_flags = IBV_SEND_SIGNALED;
          wr.wr.rdma.remote_addr = (uintptr_t)(conn->peer_mr.addr + 1);
          wr.wr.rdma.rkey = conn->peer_mr.rkey;

          sge.addr = (uintptr_t)conn->rdma_remote_region;
          sge.length = 3;
          sge.lkey = conn->rdma_remote_mr->lkey;
	gettimeofday(&finish, 0);//计时结束
          sleep(5);
          time_count(start,finish);
          gettimeofday(&start, 0);//计时开始
          TEST_NZ(ibv_post_send(conn->qp,&wr,&bad_wr));
          
        }
        else if(conn->send_state == SS_RDMA_SENT)
        {
          if(1)//conn->mode == M_WRITE)  //read-after-write
          { 
            gettimeofday(&finish, 0);//计时结束
            time_count(start,finish);
            // if(* (conn->rdma_local_region + BUFFER_SIZE + 1) != * (conn->rdma_local_region + BUFFER_SIZE - 1))
            //   TIPS(read_error!);
            // else {TIPS(durable!!!!!!!!!!!!!!!!);}
            printf("read= %s\n",(conn->rdma_remote_region));
            conn->send_msg->type = MSG_DONE;
            
            send_message(conn);
            post_receives_msg(conn);
            
          }
        }
      }
    }
  }
}

void die(const char *reason)
{
  fprintf(stderr, "%s\n", reason);
  exit(EXIT_FAILURE);
}
