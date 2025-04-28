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


        int poolSize = batchProperties.getPartitionSize();
        log.info("Configuring Async Processor Task Executor with pool size: {}", poolSize);
        executor.setCorePoolSize(poolSize);
        executor.setMaxPoolSize(poolSize);

        executor.setThreadNamePrefix("async-proc-");

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
        asyncProcessor.setTaskExecutor(taskExecutor);

        return asyncProcessor;
    }


    @Bean
    @Qualifier("asyncItemWriter")
    public AsyncItemWriter<Transaction> asyncItemWriter(
            @Qualifier("transactionWriter") ItemWriter<Transaction> delegateWriter) {

        AsyncItemWriter<Transaction> asyncWriter = new AsyncItemWriter<>();
        asyncWriter.setDelegate(delegateWriter);

        return asyncWriter;
    }


    @Bean
    @Qualifier("asyncProcessingStep")
    public Step asyncProcessingStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager,
            BatchProperties batchProperties,
            @Qualifier("PagingReader") ItemReader<Transaction> reader,
            @Qualifier("asyncItemProcessor") AsyncItemProcessor<Transaction, Transaction> asyncProcessor,
            @Qualifier("asyncItemWriter") AsyncItemWriter<Transaction> asyncWriter,
            SimpleStepTimingListener stepListener,
            SimpleChunkListener chunkListener) {

        return new StepBuilder("asyncProcessingStep", jobRepository)

                .<Transaction, Future<Transaction>>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(reader)
                .processor(asyncProcessor)
                .writer(asyncWriter)
                .listener(stepListener)
                .listener(chunkListener)

                .build();
    }
}
