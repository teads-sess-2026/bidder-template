package com.teads.summerschool.creative;

import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * In-memory copy of this bidder's creative catalog. Creative definitions (targeting,
 * max bid price) never change after CreativeSeeder seeds them — only budget mutates,
 * and bid() never reads Creative.getBudget() (remaining budget lives in Redis, see
 * BidderStatsCache) — so loading this once, right after seeding, is enough. Removes a
 * Postgres round trip from every single bid() call.
 */
@Component
public class CreativeCache {

    private static final Logger log = LoggerFactory.getLogger(CreativeCache.class);

    private final CreativeRepository repository;
    private final BidderProperties properties;
    private volatile List<Creative> creatives = List.of();

    public CreativeCache(CreativeRepository repository, BidderProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    public List<Creative> getAll() {
        return creatives;
    }

    /** Reload from Postgres. Called once by CreativeSeeder right after it seeds/verifies the catalog. */
    public void refresh() {
        creatives = List.copyOf(repository.findByBidderId(properties.getId()));
        log.info("Creative cache loaded: {} creatives", creatives.size());
    }
}
