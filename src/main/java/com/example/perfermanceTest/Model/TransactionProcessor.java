package com.example.perfermanceTest.Model;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

@Slf4j

public class TransactionProcessor implements ItemProcessor<Transaction, Transaction> {

    private final ConcurrentMap<String, LongAdder> threadProcessingCounts = new ConcurrentHashMap<>();

    @Override
    public Transaction process(final Transaction transaction) throws Exception {

        String threadName = Thread.currentThread().getName();

        log.warn("Processing transaction {} by thread : {}", transaction.getId(), threadName);
        threadProcessingCounts.computeIfAbsent(threadName, k -> new LongAdder()).increment();

       /* if(transaction.getId().equals(13)) {
            throw new RuntimeException("Transaction date is null");
        }*/



             Thread.sleep(5); // Simulate 1ms processing time if needed


        return transaction;
    }


    @PreDestroy
    public void logFinalThreadCounts() {
        if (threadProcessingCounts.isEmpty()) {
            log.info("No processing occurred or counts were not tracked.");
            return;
        }
        log.info("--- Final Item Processing Counts per Worker Thread ---");
        // Calculate total for verification
        long totalProcessed = threadProcessingCounts.values().stream()
                .mapToLong(LongAdder::longValue)
                .sum();

        threadProcessingCounts.entrySet().stream()
                .sorted(Map.Entry.comparingByKey()) // Sort by thread name for consistent output
                .forEach(entry -> {
                    log.info("Thread [{}]: Processed {} items", entry.getKey(), entry.getValue().longValue());
                });

        log.info("Total items processed across all threads: {}", totalProcessed);
        log.info("----------------------------------------------------");
    }
}
