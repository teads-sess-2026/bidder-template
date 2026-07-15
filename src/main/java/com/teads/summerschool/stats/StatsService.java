package com.teads.summerschool.stats;

import com.teads.summerschool.bidding.BiddingService;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.CreativeRepository;
import com.teads.summerschool.notification.WinNotice;
import com.teads.summerschool.notification.WinNoticeRepository;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.stats.dto.CreativeStatsResponse;
import com.teads.summerschool.stats.dto.CreativeStatsResponse.CreativeStat;
import com.teads.summerschool.stats.dto.StatsResponse;
import com.teads.summerschool.stats.dto.StatsResponse.LatencyStats;
import com.teads.summerschool.stats.dto.StatsResponse.NoBidReasons;
import com.teads.summerschool.stats.dto.StatsResponse.PacingStats;
import com.teads.summerschool.stats.dto.TargetingResponse;
import com.teads.summerschool.stats.dto.TargetingResponse.TargetingBucket;
import com.teads.summerschool.stats.dto.TimeseriesResponse;
import com.teads.summerschool.stats.dto.TimeseriesResponse.Point;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class StatsService {

    private final BidderProperties properties;
    private final BidRecordRepository bidRecordRepository;
    private final WinNoticeRepository winNoticeRepository;
    private final BiddingService biddingService;
    private final CreativeRepository creativeRepository;

    public StatsService(BidderProperties properties,
                        BidRecordRepository bidRecordRepository,
                        WinNoticeRepository winNoticeRepository,
                        BiddingService biddingService,
                        CreativeRepository creativeRepository) {
        this.properties = properties;
        this.bidRecordRepository = bidRecordRepository;
        this.winNoticeRepository = winNoticeRepository;
        this.biddingService = biddingService;
        this.creativeRepository = creativeRepository;
    }

    public Mono<StatsResponse> getStats() {
        return Mono.zip(
                bidRecordRepository.count(),
                bidRecordRepository.countByBidPriceIsNotNull(),
                bidRecordRepository.avgBidPrice().defaultIfEmpty(0.0),
                winNoticeRepository.count(),
                winNoticeRepository.sumClearingPrice().defaultIfEmpty(0.0),
                winNoticeRepository.avgClearingPrice().defaultIfEmpty(0.0)
        ).flatMap(t -> {
            long totalAuctions = t.getT1();
            long bids = t.getT2();
            double avgBid = t.getT3();
            long wins = t.getT4();
            double totalSpend = t.getT5();
            double avgWin = t.getT6();

            return Mono.zip(
                    bidRecordRepository.countGroupByNoBidReason()
                            .collectMap(r -> r.noBidReason(), r -> r.cnt()),
                    bidRecordRepository.findAllLatenciesSorted().collectList(),
                    bidRecordRepository.findFirstCreatedAt().defaultIfEmpty(LocalDateTime.now()),
                    biddingService.getRemainingBudget()
            ).map(t2 -> {
                Map<String, Long> reasons = t2.getT1();
                List<Integer> latencies = t2.getT2();
                LocalDateTime firstAt = t2.getT3();
                double remaining = t2.getT4();

                double bidRate = totalAuctions > 0 ? round4((double) bids / totalAuctions) : 0.0;
                double winRate = bids > 0 ? round4((double) wins / bids) : 0.0;
                double winRatePerAuction = totalAuctions > 0 ? round4((double) wins / totalAuctions) : 0.0;

                // no_matching_creative is what BiddingService actually writes; map it to targeting_miss
                long budgetExhausted = reasons.getOrDefault("budget_exhausted", 0L);
                long noEligibleCreative = reasons.getOrDefault("no_eligible_creative", 0L);
                long targetingMiss = reasons.getOrDefault("targeting_miss", 0L)
                        + reasons.getOrDefault("no_matching_creative", 0L);

                return new StatsResponse(
                        properties.getId(),
                        LocalDateTime.now(),
                        totalAuctions,
                        bids,
                        totalAuctions - bids,
                        bidRate,
                        wins,
                        winRate,
                        winRatePerAuction,
                        round4(avgBid),
                        round4(avgWin),
                        round4(totalSpend),
                        remaining,
                        properties.getBudget(),
                        computeLatencyStats(latencies),
                        new NoBidReasons((int) budgetExhausted, (int) noEligibleCreative, (int) targetingMiss),
                        computePacing(totalSpend, remaining, properties.getBudget(), firstAt)
                );
            });
        });
    }

    public Mono<CreativeStatsResponse> getCreativeStats(String creativeId, String sort, String order) {
        Mono<List<BidRecord>> bidsMono = bidRecordRepository.findByBidPriceIsNotNull().collectList();
        Mono<List<WinNotice>> winsMono = winNoticeRepository.findAll().collectList();
        Mono<Map<String, String>> namesMono = creativeRepository.findByBidderId(properties.getId())
                .collectMap(c -> c.getId(), c -> c.getName());

        return Mono.zip(bidsMono, winsMono, namesMono).flatMap(t -> {
            List<BidRecord> allBids = t.getT1();
            List<WinNotice> allWins = t.getT2();
            Map<String, String> names = t.getT3();

            Set<String> winRequestIds = allWins.stream()
                    .map(WinNotice::getRequestId)
                    .collect(Collectors.toSet());

            Mono<Map<String, BidRecord>> winBidRecordsMono = winRequestIds.isEmpty()
                    ? Mono.just(Map.of())
                    : bidRecordRepository.findAllByRequestIdIn(winRequestIds)
                            .collectMap(BidRecord::getRequestId);

            return winBidRecordsMono.map(winBidRecords -> {
                Map<String, List<BidRecord>> bidsByCreative = allBids.stream()
                        .filter(r -> r.getCreativeId() != null)
                        .collect(Collectors.groupingBy(BidRecord::getCreativeId));

                // clearing prices per creative, resolved via request_id → bid_record → creative_id
                Map<String, List<Double>> clearingByCreative = new HashMap<>();
                for (WinNotice win : allWins) {
                    BidRecord br = winBidRecords.get(win.getRequestId());
                    if (br != null && br.getCreativeId() != null) {
                        clearingByCreative
                                .computeIfAbsent(br.getCreativeId(), k -> new ArrayList<>())
                                .add(win.getClearingPrice());
                    }
                }

                Set<String> creativeIds = new HashSet<>(bidsByCreative.keySet());
                creativeIds.addAll(clearingByCreative.keySet());
                if (creativeId != null) {
                    creativeIds = creativeIds.contains(creativeId) ? Set.of(creativeId) : Set.of();
                }

                List<CreativeStat> stats = new ArrayList<>();
                for (String cid : creativeIds) {
                    List<BidRecord> bids = bidsByCreative.getOrDefault(cid, List.of());
                    List<Double> clearing = clearingByCreative.getOrDefault(cid, List.of());

                    long bidCount = bids.size();
                    long winCount = clearing.size();
                    double winRate = bidCount > 0 ? round4((double) winCount / bidCount) : 0.0;
                    double avgBidPrice = round4(bids.stream()
                            .mapToDouble(r -> r.getBidPrice() != null ? r.getBidPrice() : 0.0)
                            .average().orElse(0.0));
                    double avgWinPrice = clearing.isEmpty() ? 0.0
                            : round4(clearing.stream().mapToDouble(Double::doubleValue).average().orElse(0.0));
                    double spend = round4(clearing.stream().mapToDouble(Double::doubleValue).sum());

                    stats.add(new CreativeStat(cid, names.get(cid), bidCount, winCount, winRate,
                            avgBidPrice, avgWinPrice, spend));
                }

                Comparator<CreativeStat> cmp = switch (sort) {
                    case "wins"     -> Comparator.comparingLong(CreativeStat::wins);
                    case "bids"     -> Comparator.comparingLong(CreativeStat::bids);
                    case "bid_rate" -> Comparator.comparingLong(CreativeStat::bids);
                    case "win_rate" -> Comparator.comparingDouble(CreativeStat::winRate);
                    default         -> Comparator.comparingDouble(CreativeStat::spend); // spend
                };
                if ("desc".equals(order)) cmp = cmp.reversed();
                stats.sort(cmp);

                return new CreativeStatsResponse(properties.getId(), stats);
            });
        });
    }

    public Mono<TargetingResponse> getTargetingStats(String dimension) {
        Mono<List<BidRecord>> bidsMono = bidRecordRepository.findByBidPriceIsNotNull().collectList();
        Mono<List<WinNotice>> winsMono = winNoticeRepository.findAll().collectList();

        return Mono.zip(bidsMono, winsMono).flatMap(t -> {
            List<BidRecord> allBids = t.getT1();
            List<WinNotice> allWins = t.getT2();

            Set<String> winRequestIds = allWins.stream()
                    .map(WinNotice::getRequestId)
                    .collect(Collectors.toSet());

            Mono<Map<String, BidRecord>> winBidRecordsMono = winRequestIds.isEmpty()
                    ? Mono.just(Map.of())
                    : bidRecordRepository.findAllByRequestIdIn(winRequestIds)
                            .collectMap(BidRecord::getRequestId);

            return winBidRecordsMono.map(winBidRecords -> {
                Map<String, List<BidRecord>> byGeo = groupBy(allBids, BidRecord::getGeo);
                Map<String, List<BidRecord>> byDevice = groupBy(allBids, BidRecord::getDeviceType);
                Map<String, List<BidRecord>> bySegment = groupBy(allBids, BidRecord::getAudienceSegment);

                Map<String, Long> winsByGeo = new HashMap<>();
                Map<String, Long> winsByDevice = new HashMap<>();
                Map<String, Long> winsBySegment = new HashMap<>();
                for (WinNotice win : allWins) {
                    BidRecord br = winBidRecords.get(win.getRequestId());
                    if (br == null) continue;
                    if (br.getGeo() != null)             winsByGeo.merge(br.getGeo(), 1L, Long::sum);
                    if (br.getDeviceType() != null)      winsByDevice.merge(br.getDeviceType(), 1L, Long::sum);
                    if (br.getAudienceSegment() != null) winsBySegment.merge(br.getAudienceSegment(), 1L, Long::sum);
                }

                boolean all = "all".equals(dimension);
                return new TargetingResponse(
                        properties.getId(),
                        all || "geo".equals(dimension)     ? buildBuckets(byGeo, winsByGeo)         : List.of(),
                        all || "device".equals(dimension)  ? buildBuckets(byDevice, winsByDevice)   : List.of(),
                        all || "segment".equals(dimension) ? buildBuckets(bySegment, winsBySegment) : List.of()
                );
            });
        });
    }

    public Mono<TimeseriesResponse> getTimeseries(int windowMinutes, int bucketSeconds) {
        int clampedWindow = Math.max(1, Math.min(180, windowMinutes));
        int clampedBucket = Math.max(10, bucketSeconds);
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime windowStart = now.minusMinutes(clampedWindow);

        Mono<List<BidRecord>> bidsMono = bidRecordRepository.findByCreatedAtAfter(windowStart).collectList();
        Mono<List<WinNotice>> winsMono = winNoticeRepository.findByReceivedAtAfter(windowStart).collectList();

        return Mono.zip(bidsMono, winsMono).map(t -> {
            List<BidRecord> bids = t.getT1();
            List<WinNotice> wins = t.getT2();

            int bucketCount = (int) Math.floor((long) clampedWindow * 60 / clampedBucket);
            List<Point> points = new ArrayList<>(bucketCount);

            for (int i = 0; i < bucketCount; i++) {
                LocalDateTime bucketStart = windowStart.plusSeconds((long) i * clampedBucket);
                LocalDateTime bucketEnd = bucketStart.plusSeconds(clampedBucket);

                List<BidRecord> bucketRecords = bids.stream()
                        .filter(r -> !r.getCreatedAt().isBefore(bucketStart) && r.getCreatedAt().isBefore(bucketEnd))
                        .toList();
                List<WinNotice> bucketWins = wins.stream()
                        .filter(w -> !w.getReceivedAt().isBefore(bucketStart) && w.getReceivedAt().isBefore(bucketEnd))
                        .toList();

                long auctions = bucketRecords.size();
                long bidCount = bucketRecords.stream().filter(r -> r.getBidPrice() != null).count();
                long winCount = bucketWins.size();
                double bidRate = auctions > 0 ? round4((double) bidCount / auctions) : 0.0;
                double winRate = bidCount > 0 ? round4((double) winCount / bidCount) : 0.0;
                double avgBidPrice = bidCount > 0
                        ? round4(bucketRecords.stream()
                                .filter(r -> r.getBidPrice() != null)
                                .mapToDouble(BidRecord::getBidPrice)
                                .average().orElse(0.0))
                        : 0.0;
                double spend = round4(bucketWins.stream().mapToDouble(WinNotice::getClearingPrice).sum());

                points.add(new Point(bucketStart, auctions, bidCount, winCount, bidRate, winRate, avgBidPrice, spend));
            }

            return new TimeseriesResponse(properties.getId(), clampedWindow, clampedBucket, points);
        });
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static Map<String, List<BidRecord>> groupBy(List<BidRecord> records,
                                                         Function<BidRecord, String> keyFn) {
        return records.stream()
                .filter(r -> keyFn.apply(r) != null)
                .collect(Collectors.groupingBy(keyFn));
    }

    private static List<TargetingBucket> buildBuckets(Map<String, List<BidRecord>> bidsByKey,
                                                       Map<String, Long> winsByKey) {
        return bidsByKey.entrySet().stream()
                .map(e -> {
                    String key = e.getKey();
                    List<BidRecord> bids = e.getValue();
                    long bidCount = bids.size();
                    long wins = winsByKey.getOrDefault(key, 0L);
                    double winRate = bidCount > 0 ? round4((double) wins / bidCount) : 0.0;
                    double avgBidPrice = round4(bids.stream()
                            .mapToDouble(r -> r.getBidPrice() != null ? r.getBidPrice() : 0.0)
                            .average().orElse(0.0));
                    return new TargetingBucket(key, bidCount, wins, winRate, avgBidPrice);
                })
                .sorted(Comparator.comparingLong(TargetingBucket::bids).reversed())
                .toList();
    }

    private static LatencyStats computeLatencyStats(List<Integer> sorted) {
        if (sorted.isEmpty()) return new LatencyStats(0.0, 0, 0, 0, 0);
        int count = sorted.size();
        double avg = sorted.stream().mapToInt(i -> i).average().orElse(0.0);
        int p50 = sorted.get(Math.min((int)(count * 0.50), count - 1));
        int p95 = sorted.get(Math.min((int)(count * 0.95), count - 1));
        int max = sorted.get(count - 1);
        return new LatencyStats(round4(avg), p50, p95, max, count);
    }

    private static PacingStats computePacing(double totalSpend, double remainingBudget,
                                              double budget, LocalDateTime firstAt) {
        long elapsedSeconds = ChronoUnit.SECONDS.between(firstAt, LocalDateTime.now());
        double elapsedMinutes = elapsedSeconds / 60.0;
        double spendPerMinute = elapsedMinutes > 0 ? round4(totalSpend / elapsedMinutes) : 0.0;
        Double projected = (spendPerMinute > 0 && remainingBudget > 0)
                ? round4(remainingBudget / spendPerMinute) : null;
        double utilization = budget > 0 ? round4(totalSpend / budget) : 0.0;
        return new PacingStats(spendPerMinute, round4(elapsedMinutes), projected, utilization);
    }

    private static double round4(double value) {
        return Math.round(value * 10000.0) / 10000.0;
    }
}
