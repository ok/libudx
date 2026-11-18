#include "../include/udx.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

// the pacer refills its token bucket from the time elapsed since the last refill. a timer that
// runs late, because a callback blocked the loop, must not release everything owed since then:
// one refill releases at most two send quanta, a quantum being one millisecond at the pacing rate.
// the receiver never acks, so the pacing rate and the window set here stay as they are, and the
// window is large enough that only the pacer limits what one loop pass sends.

#define PACING_PACKETS_PER_MS 10

uv_loop_t loop;
udx_t udx;

udx_socket_t send_sock;
udx_socket_t recv_sock;
udx_stream_t stream;

udx_stream_write_t *req;
uv_timer_t block_timer;
uv_timer_t done_timer;
uv_check_t pass_check;

struct sockaddr_in send_addr;
struct sockaddr_in recv_addr;

static char data[4 * 1024 * 1024];

bool blocked = false;
uint64_t last_packets_tx = 0;
uint64_t max_pass = 0;
uint64_t max_pass_after_block = 0;

static void
on_stream_close (udx_stream_t *s, int status) {
  (void) s;
  (void) status;

  udx_socket_close(&send_sock);
  udx_socket_close(&recv_sock);
}

static void
on_pass (uv_check_t *handle) {
  (void) handle;

  uint64_t pass = stream.packets_tx - last_packets_tx;
  last_packets_tx = stream.packets_tx;

  if (pass > max_pass) max_pass = pass;
  if (blocked && pass > max_pass_after_block) max_pass_after_block = pass;
}

static void
on_block (uv_timer_t *timer) {
  (void) timer;

  // the pacer's refill timer is due while the loop is blocked
  uv_sleep(50);
  blocked = true;
}

static void
on_done (uv_timer_t *timer) {
  (void) timer;

  uint64_t packet = stream.mtu - UDX_IPV4_HEADER_SIZE;
  uint64_t quantum = PACING_PACKETS_PER_MS * packet;

  // a loop pass may refill twice, once on the timers' clock and once after polling
  uint64_t bound = 2 * (2 * quantum / packet + 1);

  printf("stream-pacing-late-timer: max packets in one pass=%llu (after the block %llu), bound=%llu, sent=%llu\n", (unsigned long long) max_pass, (unsigned long long) max_pass_after_block, (unsigned long long) bound, (unsigned long long) stream.packets_tx);

  assert(blocked);
  assert(max_pass_after_block > 0);
  assert(max_pass <= bound);

  // the window never limited the sender
  assert(stream.packets_tx < stream.cwnd);

  uv_close((uv_handle_t *) &block_timer, NULL);
  uv_close((uv_handle_t *) &done_timer, NULL);
  uv_close((uv_handle_t *) &pass_check, NULL);
  udx_stream_destroy(&stream);
}

int
main () {
  int e;

  req = malloc(udx_stream_write_sizeof(1));

  uv_loop_init(&loop);

  e = udx_init(&loop, &udx, NULL);
  assert(e == 0);

  e = udx_socket_init(&udx, &send_sock, NULL);
  assert(e == 0);

  e = udx_socket_init(&udx, &recv_sock, NULL);
  assert(e == 0);

  uv_ip4_addr("127.0.0.1", 18211, &send_addr);
  e = udx_socket_bind(&send_sock, (struct sockaddr *) &send_addr, 0);
  assert(e == 0);

  uv_ip4_addr("127.0.0.1", 18212, &recv_addr);
  e = udx_socket_bind(&recv_sock, (struct sockaddr *) &recv_addr, 0);
  assert(e == 0);

  e = udx_stream_init(&udx, &stream, 1, on_stream_close, NULL);
  assert(e == 0);

  e = udx_stream_connect(&stream, &send_sock, 2, (struct sockaddr *) &recv_addr);
  assert(e == 0);

  stream.cwnd = 3000;
  stream.pacing_bytes_per_ms = PACING_PACKETS_PER_MS * (stream.mtu - UDX_IPV4_HEADER_SIZE);

  uv_check_init(&loop, &pass_check);
  uv_check_start(&pass_check, on_pass);

  uv_timer_init(&loop, &block_timer);
  uv_timer_start(&block_timer, on_block, 30, 0);

  uv_timer_init(&loop, &done_timer);
  uv_timer_start(&done_timer, on_done, 120, 0);

  uv_buf_t buf = uv_buf_init(data, sizeof(data));
  udx_stream_write(req, &stream, &buf, 1, NULL);

  e = uv_run(&loop, UV_RUN_DEFAULT);
  assert(e == 0);

  e = uv_loop_close(&loop);
  assert(e == 0);

  free(req);

  return 0;
}
