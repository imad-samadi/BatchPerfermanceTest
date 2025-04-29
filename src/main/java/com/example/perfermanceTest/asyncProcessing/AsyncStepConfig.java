package com.example.perfermanceTest.asyncProcessing; // Or your chosen configuration package

import com.example.perfermanceTest.BatchProperties;
import com.example.perfermanceTest.Listeners.SimpleChunkListener;
import com.example.perfermanceTest.Listeners.SimpleStepTimingListener;
import com.example.perfermanceTest.Model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.integration.async.AsyncItemWriter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.Future;

/**
 * Configuration for asynchronous processing in Spring Batch.
 * Only the processor is parallelized; reader and writer remain single-threaded.
 */
@Configuration
@RequiredArgsConstructor // Injects final fields via constructor
@Slf4j
public class AsyncStepConfig {

    /**
     * Spring Batch repository for storing job and step metadata.
     */
    private final JobRepository jobRepository;

    /**
     * Transaction manager used to commit/rollback chunk processing.
     */
    private final PlatformTransactionManager transactionManager;

    /**
     * Externalized properties for batch configuration (chunk size, pool size, etc.).
     */
    private final BatchProperties batchProperties;

    /**
     * TaskExecutor bean for executing processor logic asynchronously.
     * Uses a fixed-size thread pool based on batchProperties.partitionSize.
     */
    @Bean
    @Qualifier("asyncProcessorTaskExecutor") // Descriptive qualifier
    public TaskExecutor asyncProcessorTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // Determine pool size (recommended to match CPU cores for CPU-bound tasks)
        int poolSize = batchProperties.getCorePoolSize();
        log.info("Configuring Async Processor Task Executor with pool size: {}", poolSize);
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);

        // Prefix thread names for easier debugging
        executor.setThreadNamePrefix("async-proc-");

        executor.initialize();
        return executor;
    }

    /**
     * Wraps the synchronous ItemProcessor in an AsyncItemProcessor.
     * Each call to process(...) is submitted to the taskExecutor.
     */
    @Bean
    @Qualifier("asyncItemProcessor")
    public AsyncItemProcessor<Transaction, Transaction> asyncItemProcessor(
            @Qualifier("transactionProcessor") ItemProcessor<Transaction, Transaction> delegateProcessor,
            @Qualifier("asyncProcessorTaskExecutor") TaskExecutor taskExecutor) {

        AsyncItemProcessor<Transaction, Transaction> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(delegateProcessor); // Original, synchronous processor
        asyncProcessor.setTaskExecutor(taskExecutor); // Asynchronous execution

        return asyncProcessor;
    }

    /**
     * Wraps the synchronous ItemWriter in an AsyncItemWriter.
     * The writer blocks on Future.get() for each item before delegating to the writer.
     */
    @Bean
    @Qualifier("asyncItemWriter")
    public AsyncItemWriter<Transaction> asyncItemWriter(
            @Qualifier("transactionWriter") ItemWriter<Transaction> delegateWriter) {

        AsyncItemWriter<Transaction> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(delegateWriter); // Original, synchronous writer

        return asyncWriter;
    }

    /**
     * Defines the Spring Batch step for asynchronous processing:
     *  - Reader and writer execute in the main thread
     *  - Processor executes in a separate thread pool
     *  - Chunk size and transaction manager controlled by configuration
     */
    @Bean
    @Qualifier("asyncProcessingStep")
    public Step asyncProcessingStep(
            @Qualifier("PagingReader") ItemReader<Transaction> reader,
            @Qualifier("asyncItemProcessor") AsyncItemProcessor<Transaction, Transaction> asyncProcessor,
            @Qualifier("asyncItemWriter") AsyncItemWriter<Transaction> asyncWriter,
            SimpleStepTimingListener stepListener,
            SimpleChunkListener chunkListener) {

        return new StepBuilder("asyncProcessingStep", jobRepository)
                // Note: output type is Future<Transaction> due to AsyncItemProcessor
                .<Transaction, Future<Transaction>>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(reader)                   // Single-threaded reader
                .processor(asyncProcessor)        // Async processing
                .writer(asyncWriter)              // Async writer unwrapping Futures
                .listener(stepListener)           // Measure step execution time
                .listener(chunkListener)          // Measure chunk execution time
                .build();                          // No taskExecutor here: processor itself is async
    }

    // Note: Reader and Writer beans are reused from existing configuration;
    // no need to reimplement or duplicate their logic here.
}
