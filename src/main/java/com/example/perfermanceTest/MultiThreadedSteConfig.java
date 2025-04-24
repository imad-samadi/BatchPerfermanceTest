package com.example.perfermanceTest;

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

@Configuration
@RequiredArgsConstructor
public class MultiThreadedSteConfig {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchProperties batchProperties;

    @Bean
    @Qualifier("multiThreadStepExecutor")
    public TaskExecutor multiThreadStepExecutor() {
        ThreadPoolTaskExecutor exec = new ThreadPoolTaskExecutor();
        exec.setCorePoolSize(batchProperties.getCorePoolSize());
        exec.setMaxPoolSize(batchProperties.getCorePoolSize());
        exec.setThreadFactory(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);          // ← now a daemon thread
            return t;
        });
        exec.initialize();
        return exec;
    }

    @Bean
    @Qualifier("multiThreadedStep")
    public Step multiThreadedStep(

            @Qualifier("PagingReader") ItemReader<Transaction> transactionReader, // Use bean name from DbToJSONConfig
            ItemProcessor<Transaction, Transaction> transactionProcessor,      // Inject your processor bean
            ItemWriter<Transaction> transactionWriter,                      // Inject your writer bean
            SimpleStepTimingListener stepTimingListener,                      // Inject step listener
            SimpleChunkListener chunkListener) {                              // Inject chunk listener

        return new StepBuilder("multiThreadedStep", jobRepository)
                .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), transactionManager) // Configure chunk size
                .reader(transactionReader)
                .processor(transactionProcessor)
                .writer(transactionWriter)
                .listener(stepTimingListener)
                .listener(chunkListener)
                .taskExecutor(multiThreadStepExecutor())
                .build();
    }

}
