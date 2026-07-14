package com.teads.summerschool.record;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Buffers BidRecords in memory and flushes them to Postgres in one batch per tick,
 * instead of one INSERT+COMMIT per bid() call. Against a remote (WAN-latency) DB, a
 * synchronous save on every request adds a full round trip to every single bid; this
 * takes persistence off the request's critical path and coalesces many writes into one.
 */
@Component
public class BidRecordBuffer {

    private static final Logger log = LoggerFactory.getLogger(BidRecordBuffer.class);

    private final BidRecordRepository repository;
    private final ConcurrentLinkedQueue<BidRecord> queue = new ConcurrentLinkedQueue<>();

    public BidRecordBuffer(BidRecordRepository repository) {
        this.repository = repository;
    }

    /** Enqueue a record for the next flush. Never touches the DB — safe to call from the hot path. */
    public void enqueue(BidRecord record) {
        queue.add(record);
    }

    @Scheduled(fixedDelay = 1000)
    public void flush() {
        List<BidRecord> batch = new ArrayList<>();
        BidRecord r;
        while ((r = queue.poll()) != null) {
            batch.add(r);
        }
        if (batch.isEmpty()) return;
        try {
            repository.saveAll(batch);
            log.debug("Flushed {} bid records", batch.size());
        } catch (Exception e) {
            log.warn("Failed to flush {} bid records — dropping batch: {}", batch.size(), e.getMessage());
        }
    }

    @PreDestroy
    void flushOnShutdown() {
        flush();
    }
}
