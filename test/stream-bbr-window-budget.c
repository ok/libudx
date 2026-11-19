#include "../include/udx.h"
#include "../src/endian.h"
#include "../src/win_filter_f64.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// in PROBE_BW the window target is cwnd_gain x bw x min_rtt. on a path whose acks come
// back later than the minimum round trip, a target of only that much holds less than one
// loaded round trip of data, the delivery rate falls below bw, and the window shrinks with it
// every round. the bdp is rounded up and three send quanta are added on top (Linux BBRv1
// bbr_bdp and bbr_quantization_budget), so the target covers bw x (min_rtt + ack delay).
// the stream is put into PROBE_BW with a fixed bw estimate and a 1 ms minimum, and the ack
// for the first packet is sent 4 ms late.

#define BW_PACKETS_PER_MS 10.3
#define MIN_RTT_MS        1
#define ACK_DELAY_MS      4

uv_loop_t loop;
udx_t udx;

udx_socket_t send_sock;
udx_socket_t recv_sock;
udx_stream_t stream;

udx_stream_write_t *req;
uv_check_t check_handle;

struct sockaddr_in send_addr;
struct sockaddr_in recv_addr;

char data[100000];

typedef struct {
  uint8_t magic;
  uint8_t version;
  uint8_t type;
  uint8_t data_offset;
  uint32_t id;
  uint32_t rwnd;
  uint32_t seq;
  uint32_t ack;
} packet_t;

static void
on_stream_close (udx_stream_t *s, int status) {
  (void) s;
  (void) status;

  udx_socket_close(&send_sock);
  udx_socket_close(&recv_sock);
}

udx_socket_send_t ack_req;

static void
on_check (uv_check_t *handle) {
  // runs in the loop pass that processed the ack
  if (stream.remote_acked == 0) return;

  uint32_t mss = stream.mtu - UDX_IPV4_HEADER_SIZE;
  uint32_t quantum = stream.pacing_bytes_per_ms / mss;

  double bdp = BW_PACKETS_PER_MS * MIN_RTT_MS * stream.bbr.cwnd_gain;
  uint32_t bdp_rounded_up = bdp;
  if (bdp_rounded_up < bdp) bdp_rounded_up++;

  uint32_t loaded_rtt = BW_PACKETS_PER_MS * (MIN_RTT_MS + ACK_DELAY_MS) + 1;

  printf("stream-bbr-window-budget: cwnd=%u bdp=%.2f quantum=%u loaded_rtt=%u state=%u\n", stream.cwnd, bdp, quantum, loaded_rtt, stream.bbr.state);

  assert(stream.bbr.state == UDX_BBR_STATE_PROBE_BW);
  assert(stream.bbr.min_rtt_ms == MIN_RTT_MS);
  assert(quantum >= 2);

  // the target is the bdp rounded up plus three quanta, and the budget is added, not multiplied
  assert(stream.cwnd >= bdp_rounded_up + 3 * quantum);
  assert(stream.cwnd <= bdp_rounded_up + 3 * quantum + 4);

  // which holds a round trip of data at the delayed ack's pace
  assert(stream.cwnd >= loaded_rtt);

  uv_close((uv_handle_t *) handle, NULL);
  udx_stream_destroy(&stream);
}

static void
on_recv (udx_socket_t *handle, ssize_t read_len, const uv_buf_t *buf, const struct sockaddr *from) {
  static bool sent = false;
  if (sent || !(buf->base[2] & UDX_HEADER_DATA)) return;
  sent = true;

  // a stream that has left STARTUP with a fixed bw estimate and an exact minimum round trip
  stream.bbr.full_bw_reached = true;
  stream.bbr.state = UDX_BBR_STATE_PROBE_BW;
  stream.bbr.cycle_index = 2;
  stream.bbr.pacing_gain = 1.0;
  stream.bbr.cwnd_gain = 2.0;
  stream.bbr.has_seen_rtt = true;
  stream.bbr.min_rtt_ms = MIN_RTT_MS;
  win_filter_f64_reset(&stream.bbr.bw, stream.bbr.rtt_count, BW_PACKETS_PER_MS);

  // far above the target, so the ack sets the window to the target
  stream.cwnd = 1000;

  uint64_t until = uv_hrtime() + ACK_DELAY_MS * 1000000ULL;
  while (uv_hrtime() < until)
    uv_sleep(1);

  static packet_t pkt;
  memset(&pkt, 0, sizeof(pkt));
  pkt.magic = UDX_MAGIC_BYTE;
  pkt.version = UDX_VERSION;
  pkt.id = udx__swap_uint32_if_be(1);
  pkt.rwnd = 0xffffffffu;
  pkt.seq = udx__swap_uint32_if_be(0);
  pkt.ack = udx__swap_uint32_if_be(1);

  uv_buf_t b = uv_buf_init((char *) &pkt, sizeof(pkt));
  udx_socket_send(&ack_req, &recv_sock, &b, 1, (struct sockaddr *) &send_addr, NULL);

  uv_check_start(&check_handle, on_check);
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

  uv_ip4_addr("127.0.0.1", 18221, &send_addr);
  e = udx_socket_bind(&send_sock, (struct sockaddr *) &send_addr, 0);
  assert(e == 0);

  uv_ip4_addr("127.0.0.1", 18222, &recv_addr);
  e = udx_socket_bind(&recv_sock, (struct sockaddr *) &recv_addr, 0);
  assert(e == 0);

  e = udx_stream_init(&udx, &stream, 1, on_stream_close, NULL);
  assert(e == 0);

  e = udx_stream_connect(&stream, &send_sock, 2, (struct sockaddr *) &recv_addr);
  assert(e == 0);

  e = udx_socket_recv_start(&recv_sock, on_recv);
  assert(e == 0);

  uv_check_init(&loop, &check_handle);

  uv_buf_t buf = uv_buf_init(data, sizeof(data));
  udx_stream_write(req, &stream, &buf, 1, NULL);

  e = uv_run(&loop, UV_RUN_DEFAULT);
  assert(e == 0);

  e = uv_loop_close(&loop);
  assert(e == 0);

  free(req);

  return 0;
}
