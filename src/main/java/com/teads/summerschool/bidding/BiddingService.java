package com.teads.summerschool.bidding;

import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.bidding.dto.BidResponse;
import com.teads.summerschool.bidding.dto.CreativeDto;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.Creative;
import com.teads.summerschool.creative.CreativeCache;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordBuffer;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
public class BiddingService {

    private static final Logger log = LoggerFactory.getLogger(BiddingService.class);

    private final Random random = new Random();

    private final BidderProperties properties;
    private final CreativeCache creativeCache;
    private final BidRecordBuffer bidRecordBuffer;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    // Last successfully computed budget.remaining, served when a scrape's
    // computation times out instead of blocking the scrape thread forever.
    private volatile double lastKnownBudget = 0.0;

    public BiddingService(BidderProperties properties,
                          CreativeCache creativeCache,
                          BidRecordBuffer bidRecordBuffer,
                          BidderStatsCache statsCache,
                          BidderMetrics metrics,
                          OwnBidCache ownBidCache) {
        this.properties = properties;
        this.creativeCache = creativeCache;
        this.bidRecordBuffer = bidRecordBuffer;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @PostConstruct
    void registerBudgetGauge() {
        metrics.registerGauge("budget.remaining", this::getRemainingBudgetSafe);
    }

    /**
     * getRemainingBudget() does a DB query plus one Redis call per creative — under
     * DB/Redis pool contention (e.g. remote backing services with WAN latency) it can
     * queue for a connection indefinitely. /actuator/prometheus has no timeout of its
     * own, so an unbounded gauge supplier here stalls the entire scrape response.
     * Bound it the same way /api/bid bounds biddingService.bid(), and fall back to the
     * last known value instead of blocking Prometheus forever.
     */
    private double getRemainingBudgetSafe() {
        try {
            double value = CompletableFuture.supplyAsync(this::getRemainingBudget)
                    .orTimeout(properties.getTimeoutMs(), TimeUnit.MILLISECONDS)
                    .exceptionally(ex -> lastKnownBudget)
                    .join();
            lastKnownBudget = value;
            return value;
        } catch (Exception ex) {
            return lastKnownBudget;
        }
    }

    public Optional<BidResponse> bid(BidRequest request) {
        // TODO: implement your bidding strategy
        // Hints:
        //   1. Record the request with buildRecord(request)
        //   2. Find matching creatives with matchingCreatives(request, creativeCache.getAll())
        //   3. Filter creatives whose maxBidPrice covers this floor: c.isWithinMaxBid(request.floorPrice())
        //   4. Filter creatives that still have budget: statsCache.getRemainingBudgets(ids) (batched) > 0
        //   5. Compute a bid price with computeBidPrice(request)
        //   6. Record metrics: metrics.recordRequest(), metrics.recordBid(), metrics.recordNoBid(reason)
        //   7. Call ownBidCache.record(requestId, creativeId, bidPrice) so AuctionNoticeConsumer
        //      can look this bid up without a DB round trip
        //   8. Enqueue the BidRecord with bidRecordBuffer.enqueue(record) and return
        //      Optional.of(new BidResponse(...)) or Optional.empty()
        metrics.recordRequest();
        metrics.recordNoBid("not_implemented");
        BidRecord record = buildRecord(request);
        record.setNoBidReason("not_implemented");
        long start = System.nanoTime();
        record.setLatencyMs((int) ((System.nanoTime() - start) / 1_000_000));
        metrics.recordLatency(0);
        bidRecordBuffer.enqueue(record);
        return Optional.empty();
    }

    private double computeBidPrice(BidRequest request) {
        // TODO: implement your pricing strategy
        // The bid must be above request.floorPrice().
        // Use properties.getStrategy() for tuning parameters.
        return request.floorPrice() * 1.01;
    }

    /** Total remaining budget across all this bidder's creatives. */
    public double getRemainingBudget() {
        return creativeCache.getAll().stream()
                .mapToDouble(c -> statsCache.getRemainingBudget(c.getId()))
                .sum();
    }

    /** Remaining budget per creative id. */
    public Map<String, Double> getRemainingBudgets() {
        Map<String, Double> budgets = new LinkedHashMap<>();
        for (Creative c : creativeCache.getAll()) {
            budgets.put(c.getId(), statsCache.getRemainingBudget(c.getId()));
        }
        return budgets;
    }

    private List<Creative> matchingCreatives(BidRequest request, List<Creative> all) {
        return all.stream()
                .filter(c -> c.matches(
                        request.targeting().geo(),
                        request.targeting().deviceType(),
                        request.targeting().audienceSegment()))
                .toList();
    }

    private CreativeDto toCreativeDto(Creative creative) {
        return new CreativeDto(
                creative.getId(),
                creative.getName(),
                creative.getDescription(),
                creative.getImageUrl(),
                creative.getCallToAction(),
                splitCsv(creative.getAllowedGeos()),
                splitCsv(creative.getAllowedDevices()),
                splitCsv(creative.getAudienceSegments())
        );
    }

    private List<String> splitCsv(String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private BidRecord buildRecord(BidRequest request) {
        BidRecord record = new BidRecord();
        record.setRequestId(request.requestId());
        record.setFloorPrice(request.floorPrice());
        if (request.targeting() != null) {
            record.setGeo(request.targeting().geo());
            record.setDeviceType(request.targeting().deviceType());
            record.setAudienceSegment(request.targeting().audienceSegment());
        }
        return record;
    }
}
