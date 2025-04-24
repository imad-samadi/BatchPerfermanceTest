package com.example.perfermanceTest.partition;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import com.example.perfermanceTest.BatchProperties;
import com.example.perfermanceTest.Listeners.SimpleChunkListener;
import com.example.perfermanceTest.Listeners.SimpleStepTimingListener;
import com.example.perfermanceTest.Model.Transaction;
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
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class PartitioningConfig {

    private final BatchProperties batchProperties;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager platformTransactionManager ;
    private final DataSource dataSource;


    private final ItemProcessor<Transaction, Transaction> transactionProcessor;

    private final SimpleStepTimingListener stepTimingListener;
    private final SimpleChunkListener chunkListener;



    @Bean
    @Qualifier("partitionTaskExecutor")
    public TaskExecutor partitionTaskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        int poolSize = batchProperties.getPartitionSize(); // Size based on number of partitions
        log.info("Configuring Partition Task Executor with pool size: {}", poolSize);
        taskExecutor.setCorePoolSize(poolSize);
        taskExecutor.setMaxPoolSize(poolSize);
        taskExecutor.setThreadNamePrefix("partition-worker-");
        taskExecutor.initialize();
        return taskExecutor;
    }


    @Bean
    public Partitioner columnRangePartitioner() {
        return new ColumnRangePartitioner(dataSource, "transactions", "id");
    }


    @Bean
    public Step workerStep(
            @Qualifier("partitionedReader") ItemReader<Transaction> reader, // Use step-scoped reader
            @Qualifier("partitionedWriter") ItemWriter<Transaction> writer, // Use step-scoped writer
            JobRepository workerJobRepository, // Need JobRepository here
            PlatformTransactionManager workerTransactionManager // Need TransactionManager here
    ) {

        return new StepBuilder("workerStep", workerJobRepository)
                .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), workerTransactionManager)
                .reader(reader)
                .processor(transactionProcessor)
                .writer(writer)

                .listener(stepTimingListener)
                .listener(chunkListener)
                .build();
    }

    // --- Partition Handler Bean ---
    @Bean
    public PartitionHandler partitionHandler(
            Step workerStep, // Inject the worker step definition
            @Qualifier("partitionTaskExecutor") TaskExecutor taskExecutor) { // Inject the partition executor
        TaskExecutorPartitionHandler handler = new TaskExecutorPartitionHandler();
        handler.setStep(workerStep); // Set the step to be executed by workers
        handler.setTaskExecutor(taskExecutor); // Set the thread pool for workers
        handler.setGridSize(batchProperties.getPartitionSize()); // Set number of concurrent workers
        // handler.afterPropertiesSet(); // Usually not needed, framework calls it
        log.info("Configuring Partition Handler with gridSize: {}", batchProperties.getPartitionSize());
        return handler;
    }


    @Bean
    public Step masterStep(
            Partitioner partitioner, // Inject the partitioner
            PartitionHandler partitionHandler // Inject the partition handler
    ) {
        return new StepBuilder("masterStep", jobRepository)
                .partitioner("workerStep", partitioner) // Specify worker step name and partitioner
                .partitionHandler(partitionHandler)      // Specify the handler
                .build();
    }
}
