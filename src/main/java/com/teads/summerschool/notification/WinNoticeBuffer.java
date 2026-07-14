package com.teads.summerschool.notification;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Buffers WinNotices in memory and flushes them to Postgres in one batch per tick,
 * same pattern as BidRecordBuffer — keeps AuctionNoticeConsumer.consume() off the DB
 * on the win path too, instead of one INSERT+COMMIT per win notice.
 */
@Component
public class WinNoticeBuffer {

    private static final Logger log = LoggerFactory.getLogger(WinNoticeBuffer.class);

    private final WinNoticeRepository repository;
    private final ConcurrentLinkedQueue<WinNotice> queue = new ConcurrentLinkedQueue<>();

    public WinNoticeBuffer(WinNoticeRepository repository) {
        this.repository = repository;
    }

    public void enqueue(WinNotice notice) {
        queue.add(notice);
    }

    @Scheduled(fixedDelay = 1000)
    public void flush() {
        List<WinNotice> batch = new ArrayList<>();
        WinNotice n;
        while ((n = queue.poll()) != null) {
            batch.add(n);
        }
        if (batch.isEmpty()) return;
        try {
            repository.saveAll(batch);
            log.debug("Flushed {} win notices", batch.size());
        } catch (Exception e) {
            log.warn("Failed to flush {} win notices — dropping batch: {}", batch.size(), e.getMessage());
        }
    }

    @PreDestroy
    void flushOnShutdown() {
        flush();
    }
}
