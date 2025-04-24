package com.example.perfermanceTest.config; // Or your chosen configuration package

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

@Configuration
@RequiredArgsConstructor // Injects final fields via constructor
@Slf4j
public class AsyncStepConfig {

    // Inject necessary beans via constructor
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchProperties batchProperties;


    @Bean
    @Qualifier("asyncProcessorTaskExecutor") // Use a descriptive qualifier
    public TaskExecutor asyncProcessorTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // Ensure this method exists in your BatchProperties or use a fixed value/different property
        // Example: Using partitionSize if corePoolSize isn't defined, but ideally add corePoolSize
        int poolSize = batchProperties.getPartitionSize(); // Or ideally batchProperties.getCorePoolSize()
        log.info("Configuring Async Processor Task Executor with pool size: {}", poolSize);
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize); // Fixed size pool often suitable for batch
        // executor.setQueueCapacity(...) // Consider queue capacity if needed
        executor.setThreadNamePrefix("async-proc-");
        // Removed the setThreadFactory for daemon threads - default non-daemon is usually preferred
        executor.initialize();
        return executor;
    }


    @Bean
    @Qualifier("asyncItemProcessor")
    public AsyncItemProcessor<Transaction, Transaction> asyncItemProcessor(
            @Qualifier("transactionProcessor") ItemProcessor<Transaction, Transaction> delegateProcessor,
            @Qualifier("asyncProcessorTaskExecutor") TaskExecutor taskExecutor) { // Inject specific executor

        AsyncItemProcessor<Transaction, Transaction> asyncProcessor = new AsyncItemProcessor<>();
        asyncProcessor.setDelegate(delegateProcessor);
        asyncProcessor.setTaskExecutor(taskExecutor); // Assign the executor
        // asyncProcessor.afterPropertiesSet(); // Usually not needed, framework calls it
        return asyncProcessor;
    }


    @Bean
    @Qualifier("asyncItemWriter")
    public AsyncItemWriter<Transaction> asyncItemWriter(
            @Qualifier("transactionWriter") ItemWriter<Transaction> delegateWriter) { // Inject delegate

        AsyncItemWriter<Transaction> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(delegateWriter);
        // asyncWriter.afterPropertiesSet(); // Usually not needed
        return asyncWriter;
    }


    @Bean
    @Qualifier("asyncProcessingStep")
    public Step asyncProcessingStep(
            JobRepository jobRepository, // Inject required dependency
            PlatformTransactionManager transactionManager, // Inject required dependency
            BatchProperties batchProperties, // Inject required dependency
            @Qualifier("PagingReader") ItemReader<Transaction> reader, // Inject reader
            @Qualifier("asyncItemProcessor") AsyncItemProcessor<Transaction, Transaction> asyncProcessor, // Inject async processor
            @Qualifier("asyncItemWriter") AsyncItemWriter<Transaction> asyncWriter, // Inject async writer
            SimpleStepTimingListener stepListener, // Inject listener
            SimpleChunkListener chunkListener) {   // Inject listener

        return new StepBuilder("asyncProcessingStep", jobRepository)
                // The output of the asyncProcessor is Future<Transaction>, input to writer is also Future<Transaction>
                .<Transaction, Future<Transaction>>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(reader)
                .processor(asyncProcessor) // Use the Async processor
                .writer(asyncWriter)       // Use the Async writer
                .listener(stepListener)
                .listener(chunkListener)
                // DO NOT add .taskExecutor(...) here when using AsyncItemProcessor/Writer pattern
                .build();
    }
}
