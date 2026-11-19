#include "../include/udx.h"
#include "../src/endian.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// bbr sizes its window from the minimum round trip, so that minimum must come from a current,
// fine-grained reading. an ack read after a long callback in the same wakeup would otherwise be
// timed with the loop's cached clock, the sample would come out as 1 ms, and the window would be
// cut to a fraction for as long as that minimum stands.
// the receiver sends data that the sender reads first. the read callback blocks for 50 ms and
// the ack for P0 with P2-P4 sacked arrives 25 ms in, so it is read later in the same wakeup.

uv_loop_t loop;
udx_t udx;

udx_socket_t send_sock;
udx_socket_t recv_sock;
udx_stream_t stream;

udx_stream_write_t *req;
uv_timer_t check_timer;

struct sockaddr_in send_addr;
struct sockaddr_in recv_addr;

char data[5000]; // P0-P4

typedef struct {
  uint8_t magic;
  uint8_t version;
  uint8_t type;
  uint8_t data_offset;
  uint32_t id;
  uint32_t rwnd;
  uint32_t seq;
  uint32_t ack;
  uint32_t payload[2]; // the sack block of the ack
} packet_t;

static void
on_stream_close (udx_stream_t *s, int status) {
  (void) s;
  (void) status;

  udx_socket_close(&send_sock);
  udx_socket_close(&recv_sock);
}

udx_socket_send_t data_req;
udx_socket_send_t ack_req;

static void
send_packet (udx_socket_send_t *req, packet_t *pkt, uint8_t type, uint32_t seq, uint32_t ack, uint32_t sack_start, uint32_t sack_end) {
  memset(pkt, 0, sizeof(*pkt));
  pkt->magic = UDX_MAGIC_BYTE;
  pkt->version = UDX_VERSION;
  pkt->type = type;
  pkt->id = udx__swap_uint32_if_be(1);
  pkt->rwnd = 0xffffffffu;
  pkt->seq = udx__swap_uint32_if_be(seq);
  pkt->ack = udx__swap_uint32_if_be(ack);
  pkt->payload[0] = udx__swap_uint32_if_be(sack_start);
  pkt->payload[1] = udx__swap_uint32_if_be(sack_end);

  uv_buf_t buf = uv_buf_init((char *) pkt, sizeof(*pkt));
  udx_socket_send(req, &recv_sock, &buf, 1, (struct sockaddr *) &send_addr, NULL);
}

static void
on_recv (udx_socket_t *handle, ssize_t read_len, const uv_buf_t *buf, const struct sockaddr *from) {
  static bool sent = false;
  if (sent || !(buf->base[2] & UDX_HEADER_DATA)) return;
  sent = true;

  // read in a later millisecond than the send stamps of P0-P4, on the clock of the loop time
  uint64_t until = uv_hrtime() + 2000000;
  while (uv_hrtime() < until)
    uv_sleep(1);

  // data that does not ack P0 yet
  static packet_t pkt;
  send_packet(&data_req, &pkt, UDX_HEADER_DATA, 0, 0, 0, 0);
}

static void
on_check (uv_timer_t *timer) {
  printf("bbr-min-rtt-us: min_rtt_us=%u min_rtt_ms=%u\n", stream.bbr.min_rtt_us, stream.bbr.min_rtt_ms);

  // the ack arrived about 25 ms after P0-P4 were sent
  assert(stream.bbr.min_rtt_us != ~0U);
  assert(stream.bbr.min_rtt_us >= 20000);

  // the millisecond figure is the same minimum, rounded up
  assert(stream.bbr.min_rtt_ms == (stream.bbr.min_rtt_us + 999) / 1000);

  uv_close((uv_handle_t *) timer, NULL);
  udx_stream_destroy(&stream);
}

static void
on_read (udx_stream_t *s, ssize_t read_len, const uv_buf_t *buf) {
  if (read_len <= 0) return;

  uv_sleep(25);

  // lands on send_sock while libuv is still reading it in this wakeup
  static packet_t pkt;
  send_packet(&ack_req, &pkt, UDX_HEADER_SACK, 1, 1, 2, 5);

  uv_sleep(25);

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

  uv_ip4_addr("127.0.0.1", 18097, &send_addr);
  e = udx_socket_bind(&send_sock, (struct sockaddr *) &send_addr, 0);
  assert(e == 0);

  uv_ip4_addr("127.0.0.1", 18098, &recv_addr);
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

  uv_buf_t buf = uv_buf_init(data, sizeof(data));
  udx_stream_write(req, &stream, &buf, 1, NULL);

  e = uv_run(&loop, UV_RUN_DEFAULT);
  assert(e == 0);

  e = uv_loop_close(&loop);
  assert(e == 0);

  free(req);

  return 0;
}
