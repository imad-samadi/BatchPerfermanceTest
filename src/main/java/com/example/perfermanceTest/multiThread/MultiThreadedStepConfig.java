package com.example.perfermanceTest.multiThread;

import com.example.perfermanceTest.BatchProperties;
import com.example.perfermanceTest.Listeners.SimpleChunkListener;
import com.example.perfermanceTest.Listeners.SimpleStepTimingListener;
import com.example.perfermanceTest.Model.Transaction;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Configuration for a multi-threaded Spring Batch step
 * that reads transactions and writes to CSV in parallel.
 */
@Configuration
@RequiredArgsConstructor
public class MultiThreadedStepConfig {

    // DataSource for database connectivity (injected by Spring)
    private final DataSource dataSource;
    // Repository for storing batch metadata (executions, steps)
    private final JobRepository jobRepository;
    // Transaction manager for handling chunk transactions
    private final PlatformTransactionManager transactionManager;
    // Externalized batch properties (e.g. chunk size, thread pool size)
    private final BatchProperties batchProperties;

    /**
     * TaskExecutor configured with a fixed thread pool.
     * Threads are set as daemon so they don't block JVM shutdown.
     * CorePoolSize and MaxPoolSize are equal to the configured number of threads.
     */
    @Bean
    @Qualifier("multiThreadStepExecutor")
    public TaskExecutor multiThreadStepExecutor() {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        // Use as many threads as specified (e.g., cores count)
        exec.setCorePoolSize(batchProperties.getCorePoolSize());
        exec.setMaxPoolSize(batchProperties.getCorePoolSize());
        // Daemon threads ensure the JVM can exit when job completes
        exec.setThreadFactory(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        });
        exec.initialize();
        return exec;
    }

    /**
     * Defines the multi-threaded Step: read → process → write in parallel.
     * Each thread will handle its own chunk of items concurrently.
     */
    @Bean
    @Qualifier("multiThreadedStep")
    public Step multiThreadedStep(
            @Qualifier("PagingReader") ItemReader<Transaction> transactionReader,
            ItemProcessor<Transaction, Transaction> transactionProcessor,
            ItemWriter<Transaction> transactionWriter,
            SimpleStepTimingListener stepTimingListener,
            SimpleChunkListener chunkListener
    ) {
        return new StepBuilder("multiThreadedStep", jobRepository)
                // Configure chunk size and transaction manager
                .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(transactionReader)             // Shared, thread-safe reader
                .processor(transactionProcessor)        // Business logic processor
                .writer(transactionWriter)             // Shared, thread-safe writer
                .listener(stepTimingListener)          // Listener to time the step
                .listener(chunkListener)               // Listener to time each chunk
                .taskExecutor(multiThreadStepExecutor()) // Executes chunks in parallel threads
                .build();
    }


    // JdbcPagingItemReader is thread-safe by default.

}
