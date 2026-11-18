
#include "debug.h"
#include "internal.h"

// store current delivery information in the packet
// to generate a rate sample when it is acked
void
udx__rate_pkt_sent (udx_stream_t *stream, udx_packet_t *pkt) {
  if (stream->seq == stream->remote_acked) {
    // there is no data in flight, so start the rate
    // sample intervals of this first flight with the current time.
    stream->first_sent_us = pkt->time_sent_us;
    stream->delivered_us = pkt->time_sent_us;
    stream->delivered_ts = uv_now(stream->udx->loop);
  }

  pkt->first_sent_us = stream->first_sent_us;

  pkt->delivered_us = stream->delivered_us;
  pkt->delivered = stream->delivered;
  pkt->is_app_limited = stream->app_limited ? true : false;

  debug_throughput(stream);
}

// rate samples are timed in microseconds: a flight sent and acked within one tick of the
// loop's millisecond clock would otherwise be credited to a single millisecond.
// an interval shorter than one tick is never turned into a rate.
#define UDX_RATE_MIN_INTERVAL_US 1000

static inline uint64_t
stamp_us_delta (uint64_t t1, uint64_t t0) {
  int64_t delta = (int64_t) t1 - (int64_t) t0;
  return max_int64(delta, 0L);
}

void
udx__rate_pkt_delivered (udx_stream_t *stream, udx_packet_t *pkt, udx_rate_sample_t *rs) {

  if (!pkt->delivered_us) {
    return;
  }

  if (!rs->prior_delivered || rack_sent_after(pkt->time_sent_us, pkt->seq, stream->first_sent_us, rs->seq)) {
    rs->prior_delivered = pkt->delivered;

    rs->prior_timestamp = pkt->delivered_us;
    rs->is_app_limited = pkt->is_app_limited;
    rs->is_retrans = pkt->retransmitted;
    rs->seq = pkt->seq;

    // record send time of most recently ACKed packet
    stream->first_sent_us = pkt->time_sent_us;

    rs->interval_us = stamp_us_delta(stream->first_sent_us, pkt->first_sent_us);
  }
}

// generate a rate sample and store it in the udx_stream_t
void
udx__rate_gen (udx_stream_t *stream, uint32_t delivered, uint32_t lost, udx_rate_sample_t *rs) {
  uint64_t now_us = uv_hrtime() / 1000;

  // clear app limited if bubble is acked and gone
  if (stream->app_limited && seq_compare(stream->delivered, stream->app_limited) > 0) {
    stream->app_limited = 0;
  }

  if (delivered) {
    stream->delivered_ts = uv_now(stream->udx->loop);
    stream->delivered_us = now_us;
  }

  rs->acked_sacked = delivered;
  rs->losses = lost;

  if (!rs->prior_timestamp) {
    rs->delivered = -1;
    rs->interval_us = -1;
    return;
  }

  rs->delivered = stream->delivered - rs->prior_delivered;

  uint64_t snd_us = rs->interval_us;
  uint64_t ack_us = stamp_us_delta(now_us, rs->prior_timestamp);

  rs->interval_us = snd_us > ack_us ? snd_us : ack_us;

  rs->snd_interval_us = snd_us;
  rs->rcv_interval_us = ack_us;

  char *ca_state_string[] = {
    "Unknown",
    "UDX_CA_OPEN",
    "UDX_CA_RECOVERY",
    "UDX_CA_LOSS",
  };

  uint64_t min_interval_us = (uint64_t) udx_rtt_min(stream) * 1000;
  if (min_interval_us < UDX_RATE_MIN_INTERVAL_US) min_interval_us = UDX_RATE_MIN_INTERVAL_US;

  if ((uint64_t) rs->interval_us < min_interval_us) {
    if (!rs->is_retrans) {
      debug_printf("rs->interval_us=%" PRId64 ", rs->delivered=%d, stream->ca_state=%s, stream->min_rtt=%u\n", rs->interval_us, rs->delivered, ca_state_string[stream->ca_state], udx_rtt_min(stream));
    }
    rs->interval_us = -1;
    return;
  }

  if (!rs->is_app_limited || ((uint64_t) rs->delivered * stream->rate_interval_us >= (uint64_t) stream->rate_delivered * rs->interval_us)) {
    stream->rate_delivered = rs->delivered;
    stream->rate_interval_us = rs->interval_us;
    stream->rate_interval_ms = rs->interval_us / 1000;
    stream->rate_sample_is_app_limited = rs->is_app_limited;
  }

  debug_throughput(stream);
}

void
udx__rate_check_app_limited (udx_stream_t *stream) {
  if (stream->writes_queued_bytes < udx__max_payload(stream) &&
      stream->inflight_queue.len < stream->cwnd &&
      stream->retransmit_queue.len == 0) {
    stream->app_limited = stream->delivered + stream->inflight_queue.len ?: 1; // can't use sequence zero since it indicates no app limit.
  }
}
