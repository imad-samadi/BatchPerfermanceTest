package com.example.perfermanceTest.Listeners;

import com.example.perfermanceTest.Model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Configuration
public class MultiThreadLoggingListeners {

    @Bean
    public ItemWriteListener<Transaction> multiThreadWriteListener() {
        return new ItemWriteListener<Transaction>() {

            // beforeWrite is less useful here as it runs before the actual work in the thread

            @Override
            public void afterWrite(Chunk<? extends Transaction> items) {
                // This method is called by the specific thread AFTER it successfully
                // wrote its chunk of items.
                String threadName = Thread.currentThread().getName();

                // Extract IDs from the chunk that was just written
                List<Integer> ids = items.getItems().stream()
                        .map(Transaction::getId) // Assuming Transaction has getId() returning Integer
                        .collect(Collectors.toList());

                log.warn("Thread [{}] ---> Successfully wrote Chunk with IDs: {}", threadName, ids);
            }

            @Override
            public void onWriteError(Exception exception, Chunk<? extends Transaction> items) {
                // Called by the thread if writing its chunk failed
                String threadName = Thread.currentThread().getName();
                List<Integer> ids = items.getItems().stream()
                        .map(Transaction::getId)
                        .collect(Collectors.toList());
                log.error("Thread [{}] !!! Error writing Chunk with IDs: {}. Exception: {}",
                        threadName, ids, exception.getMessage());
            }
        };
    }
}
