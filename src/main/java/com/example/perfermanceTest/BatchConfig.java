package com.example.perfermanceTest;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.example.perfermanceTest.Listeners.SimpleJobTimingListener ;
@Configuration
public class BatchConfig {

   /* @Bean
    public Job simpleTransactionJob(
            JobRepository jobRepository,
            @Qualifier("simpleTransactionStep") Step simpleTransactionStep, // Reference the step bean defined above
            SimpleJobTimingListener jobTimingListener) {                   // Inject job listener

        return new JobBuilder("simpleTransactionJob", jobRepository)
                .incrementer(new RunIdIncrementer()) // Allows re-running with different parameters
                .listener(jobTimingListener)         // Register the job listener
                .flow(simpleTransactionStep)        // Define the sequence of steps
                .end()
                .build();
    }*/

   /*@Bean
    public Job multiThreadedStepJob(
            JobRepository jobRepository,
            @Qualifier("multiThreadedStep") Step simpleTransactionStep, // Reference the step bean defined above
            SimpleJobTimingListener jobTimingListener) {                   // Inject job listener

        return new JobBuilder("multiThreadedJob", jobRepository)
                .incrementer(new RunIdIncrementer()) // Allows re-running with different parameters
                .listener(jobTimingListener)         // Register the job listener
                .flow(simpleTransactionStep)        // Define the sequence of steps
                .end()
                .build();
    }*/

   /* @Bean
    public Job multiThreadedStepJob(
            JobRepository jobRepository,
            @Qualifier("asyncProcessingStep") Step simpleTransactionStep, // Reference the step bean defined above
            SimpleJobTimingListener jobTimingListener) {                   // Inject job listener

        return new JobBuilder("asyncProcessingJob", jobRepository)
                .incrementer(new RunIdIncrementer()) // Allows re-running with different parameters
                .listener(jobTimingListener)         // Register the job listener
                .flow(simpleTransactionStep)        // Define the sequence of steps
                .end()
                .build();
    }*/
    @Bean
    public Job partitionedJob(
            JobRepository jobRepository,
            SimpleJobTimingListener jobTimingListener,
            @Qualifier("masterStep") Step masterStep) { // Inject the MASTER step

        return new JobBuilder("partitionedJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .listener(jobTimingListener)
                .start(masterStep) // Start with the master step
                .build();
    }


}
