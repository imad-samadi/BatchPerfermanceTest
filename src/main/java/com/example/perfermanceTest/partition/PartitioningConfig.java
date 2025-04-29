package com.example.perfermanceTest.partition;

import com.example.perfermanceTest.BatchProperties;
import com.example.perfermanceTest.Listeners.SimpleChunkListener;
import com.example.perfermanceTest.Listeners.SimpleStepTimingListener;
import com.example.perfermanceTest.Model.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
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
 * Configuration class for partitioning a Spring Batch job.
 * Defines the master step, worker step, partitioner, and related threading.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class PartitioningConfig {

    // Externalized properties for batch configuration (e.g., chunk size, pool size, partition count)
    private final BatchProperties batchProperties;

    // Repository for storing Spring Batch metadata (job and step executions)
    private final JobRepository jobRepository;

    // Transaction manager used for worker step transactions
    private final PlatformTransactionManager platformTransactionManager;

    // DataSource for PartitionRangePartitioner to calculate ranges
    private final DataSource dataSource;

    // Shared processor bean for transforming Transaction items
    private final ItemProcessor<Transaction, Transaction> transactionProcessor;

    // Listeners for logging step and chunk timing
    private final SimpleStepTimingListener stepTimingListener;
    private final SimpleChunkListener chunkListener;

    /**
     * TaskExecutor for running worker step partitions in parallel.
     * The pool size is defined by batchProperties.corePoolSize.
     */
    @Bean
    @Qualifier("partitionTaskExecutor")
    public TaskExecutor partitionTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        int poolSize = batchProperties.getCorePoolSize();
        log.info("Configuring partition executor with pool size: {}", poolSize);
        taskExecutor.setCorePoolSize(poolSize);
        taskExecutor.setMaxPoolSize(poolSize);
        taskExecutor.setThreadNamePrefix("partition-worker-");
        taskExecutor.initialize();
        return taskExecutor;
    }

    /**
     * Partitioner that splits the 'transactions' table into ranges of IDs.
     * Creates ExecutionContexts containing minId/maxId for each partition.
     */
    @Bean
    public Partitioner columnRangePartitioner() {
        return new ColumnRangePartitioner(dataSource, "transactions", "id");
    }

    /**
     * Worker step definition: each partition executes this step.
     * Reads, processes, and writes Transaction items for its ID range.
     */
    @Bean
    public Step workerStep(
            @Qualifier("PagingReaderPartition") ItemReader<Transaction> reader,
            @Qualifier("partitionedWriter") ItemWriter<Transaction> writer
    ) {
        return new StepBuilder("workerStep", jobRepository)
                .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), platformTransactionManager)
                .reader(reader)                     // Step-scoped reader for this partition
                .processor(transactionProcessor)    // Business logic processor
                .writer(writer)                     // Step-scoped writer for this partition
                .listener(stepTimingListener)       // Listener for step timing
                .listener(chunkListener)            // Listener for chunk timing
                .build();                           // No taskExecutor here; handled by partition handler
    }

    /**
     * PartitionHandler that launches the workerStep in parallel using the TaskExecutor.
     * gridSize determines how many partitions run concurrently.
     */
    @Bean
    public PartitionHandler partitionHandler(
            Step workerStep,
            @Qualifier("partitionTaskExecutor") TaskExecutor taskExecutor
    ) {
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep);
        handler.setTaskExecutor(taskExecutor);
        handler.setGridSize(batchProperties.getPartitionSize());
        log.info("Configuring PartitionHandler with gridSize: {}", batchProperties.getPartitionSize());
        return handler;
    }

    /**
     * Master step definition: orchestrates partitioning by invoking the partitionHandler.
     * Uses the columnRangePartitioner to generate ExecutionContexts for workers.
     */
    @Bean
    public Step masterStep(Partitioner partitioner, PartitionHandler partitionHandler) {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("workerStep", partitioner)  // Name of worker step and the partitioner
                .partitionHandler(partitionHandler)       // Executes partitions
                .build();                                 // No commit interval here
    }
}
