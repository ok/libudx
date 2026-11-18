#include "../include/udx.h"
#include "../src/endian.h"
#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// a rate sample divides what was delivered by how long the delivery took, and that time must
// come from a real clock. acks read after a long callback in the same wakeup carry the loop's
// cached time, so on the millisecond clock a whole flight that took tens of milliseconds to
// deliver would be credited to the one or two ticks the cached time advanced.
// the sender sends a flight, the receiver waits 2 ms and sends data. the sender's read callback
// then blocks and acks the flight one packet at a time, so the acks are read later in the same
// wakeup. the bandwidth estimate must stay within a small multiple of what was really delivered.

uv_loop_t loop;
udx_t udx;

udx_socket_t send_sock;
udx_socket_t recv_sock;
udx_stream_t stream;

udx_stream_write_t *req;
uv_timer_t check_timer;

struct sockaddr_in send_addr;
struct sockaddr_in recv_addr;

char data[10000];

uint64_t flight_sent_ns;
uint64_t flight_acked_ns;
uint32_t flight_len;

typedef struct {
  uint8_t magic;
  uint8_t version;
  uint8_t type;
  uint8_t data_offset;
  uint32_t id;
  uint32_t rwnd;
  uint32_t seq;
  uint32_t ack;
  uint32_t payload[2];
} packet_t;

static void
on_stream_close (udx_stream_t *s, int status) {
  (void) s;
  (void) status;

  udx_socket_close(&send_sock);
  udx_socket_close(&recv_sock);
}

udx_socket_send_t data_req;
udx_socket_send_t ack_reqs[64];
packet_t ack_pkts[64];

static void
send_packet (udx_socket_send_t *req, packet_t *pkt, uint8_t type, uint32_t seq, uint32_t ack, size_t len) {
  memset(pkt, 0, sizeof(*pkt));
  pkt->magic = UDX_MAGIC_BYTE;
  pkt->version = UDX_VERSION;
  pkt->type = type;
  pkt->id = udx__swap_uint32_if_be(1);
  pkt->rwnd = 0xffffffffu;
  pkt->seq = udx__swap_uint32_if_be(seq);
  pkt->ack = udx__swap_uint32_if_be(ack);

  uv_buf_t buf = uv_buf_init((char *) pkt, len);
  udx_socket_send(req, &recv_sock, &buf, 1, (struct sockaddr *) &send_addr, NULL);
}

static void
on_recv (udx_socket_t *handle, ssize_t read_len, const uv_buf_t *buf, const struct sockaddr *from) {
  static bool sent = false;
  if (sent || !(buf->base[2] & UDX_HEADER_DATA)) return;
  sent = true;

  // read in a later millisecond than the send stamps of the flight, on the clock of the loop time
  uint64_t until = uv_hrtime() + 2000000;
  while (uv_hrtime() < until)
    uv_sleep(1);

  // data that does not ack the flight yet
  static packet_t pkt;
  send_packet(&data_req, &pkt, UDX_HEADER_DATA, 0, 0, sizeof(pkt));
}

static void
on_check (uv_timer_t *timer) {
  uint64_t bw = 0;
  udx_stream_get_bw(&stream, &bw);

  // what the flight really delivered, payload bytes per second over at least this long
  double elapsed_s = (flight_acked_ns - flight_sent_ns) / 1e9;
  double delivered_bps = sizeof(data) / elapsed_s;

  printf("stream-rate-sample-us: flight=%u elapsed=%.1f ms delivered=%.0f B/s bw=%" PRIu64 " B/s\n", flight_len, elapsed_s * 1000, delivered_bps, bw);

  assert(flight_len > 1);
  assert(bw > 0);
  assert(bw <= 2 * delivered_bps);

  uv_close((uv_handle_t *) timer, NULL);
  udx_stream_destroy(&stream);
}

static void
on_read (udx_stream_t *s, ssize_t read_len, const uv_buf_t *buf) {
  if (read_len <= 0) return;

  flight_len = s->seq;
  assert(flight_len <= 64);

  uv_sleep(20);

  // one ack per packet of the flight, all landing on send_sock while libuv is still reading it
  for (uint32_t i = 1; i <= flight_len; i++) {
    send_packet(&ack_reqs[i - 1], &ack_pkts[i - 1], 0, 1, i, 20);
  }

  flight_acked_ns = uv_hrtime();

  // runs after the rest of this wakeup's reads
  uv_timer_start(&check_timer, on_check, 0, 0);
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

  uv_ip4_addr("127.0.0.1", 18201, &send_addr);
  e = udx_socket_bind(&send_sock, (struct sockaddr *) &send_addr, 0);
  assert(e == 0);

  uv_ip4_addr("127.0.0.1", 18202, &recv_addr);
  e = udx_socket_bind(&recv_sock, (struct sockaddr *) &recv_addr, 0);
  assert(e == 0);

  e = udx_stream_init(&udx, &stream, 1, on_stream_close, NULL);
  assert(e == 0);

  e = udx_stream_connect(&stream, &send_sock, 2, (struct sockaddr *) &recv_addr);
  assert(e == 0);

  e = udx_stream_read_start(&stream, on_read);
  assert(e == 0);

  e = udx_socket_recv_start(&recv_sock, on_recv);
  assert(e == 0);

  uv_timer_init(&loop, &check_timer);

  flight_sent_ns = uv_hrtime();

  uv_buf_t buf = uv_buf_init(data, sizeof(data));
  udx_stream_write(req, &stream, &buf, 1, NULL);

  e = uv_run(&loop, UV_RUN_DEFAULT);
  assert(e == 0);

  e = uv_loop_close(&loop);
  assert(e == 0);

  free(req);

  return 0;
}
