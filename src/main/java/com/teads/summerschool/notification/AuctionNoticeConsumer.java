package com.teads.summerschool.notification;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.proto.AuctionNoticeProto;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuctionNoticeConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuctionNoticeConsumer.class);

    private final WinNoticeBuffer winNoticeBuffer;
    private final BidderProperties properties;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    public AuctionNoticeConsumer(WinNoticeBuffer winNoticeBuffer,
                                 BidderProperties properties,
                                 BidderStatsCache statsCache,
                                 BidderMetrics metrics,
                                 OwnBidCache ownBidCache) {
        this.winNoticeBuffer = winNoticeBuffer;
        this.properties = properties;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @KafkaListener(topics = "${kafka.topic.auction-notifications}",
            autoStartup = "${spring.kafka.listener.auto-startup:true}")
    public void consume(byte[] message) {
        try {
            AuctionNoticeProto.AuctionNotice notice = AuctionNoticeProto.AuctionNotice.parseFrom(message);

            // This topic broadcasts EVERY auction's outcome to EVERY bidder, so most
            // messages a bidder receives are ones it never bid on. Filter on the
            // in-memory OwnBidCache (see BiddingService.bid()) BEFORE touching Redis or
            // Postgres — an O(1) local lookup instead of a DB round trip on every message.
            OwnBidCache.Entry ourBid = ownBidCache.get(notice.getRequestId());
            if (ourBid == null) {
                return;
            }

            boolean won = properties.getId().equals(notice.getWinningBidderId());

            log.debug("KAFKA  id={} winner={} won={}", notice.getRequestId(), notice.getWinningBidderId(), won);

            if (won) {
                // TODO: handle win — record the win, decrement creative budget, update metrics
                // Hints:
                //   - ourBid.creativeId() / ourBid.bidPrice() is what we bid on this auction
                //   - Call statsCache.recordWin(ourBid.creativeId(), notice.getClearingPrice())
                //   - Call metrics.recordWin(notice.getClearingPrice())
                //   - Save a WinNotice via winNoticeBuffer.enqueue(...)
                log.info("** WIN  id={} creative={} clearing={} — not yet handled",
                        notice.getRequestId(), ourBid.creativeId(), notice.getClearingPrice());
            } else {
                // TODO: handle loss — update metrics
                // Hints:
                //   - ourBid.bidPrice() is what we bid; call metrics.recordLoss()
            }
        } catch (Exception e) {
            log.error("** KAFKA ERROR  failed to process auction notice: {}", e.getMessage());
        }
    }
}
