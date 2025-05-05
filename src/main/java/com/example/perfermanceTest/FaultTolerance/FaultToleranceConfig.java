package com.example.perfermanceTest.FaultTolerance;


import com.example.perfermanceTest.FaultTolerance.Listenners.CustomJobExecutionListener;
import com.example.perfermanceTest.FaultTolerance.Listenners.LoggingSkipListener;
import com.example.perfermanceTest.FaultTolerance.Test.APIProcessListener;
import com.example.perfermanceTest.FaultTolerance.Test.APIProcessor;
import com.example.perfermanceTest.FaultTolerance.WriterExecption.ExceptionThrowingWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;

import java.util.ArrayList;
import java.util.List;

@Configuration
@RequiredArgsConstructor
@Slf4j

public class FaultToleranceConfig {
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final DataSource dataSource;

    @Bean
    @Qualifier("FaultToleranceReader")
    public FlatFileItemReader<Transaction2> simpleTransactionReader() {
        log.info("Configuring simple CSV reader");
        return new FlatFileItemReaderBuilder<Transaction2>()
                .name("simpleTransactionReader")
                .resource(new FileSystemResource("C:/Users/msi/Downloads/perfermanceTest/perfermanceTest/src/main/resources/transactions.csv"))
                .linesToSkip(1) // Skip header
                .delimited()
                .names("id", "transactionDate", "amount", "createdAt") // Must match CSV header
                .fieldSetMapper(new BeanWrapperFieldSetMapper<>() {{
                    setTargetType(Transaction2.class); // Map to our Record
                }})
                .build();
    }
    @Bean("FaultToleranceWriter")
    public JdbcBatchItemWriter<Transaction2> simpleTransactionWriter() {
        log.info("Configuring simple JDBC writer");
        return new JdbcBatchItemWriterBuilder<Transaction2>()
                .dataSource(dataSource)
                .sql("INSERT INTO transactions2 (id, transaction_date, amount, created_at) VALUES (:id, :transactionDate, :amount, :createdAt)")
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .build();
    }
    //a Bean to test the writer that throws the exeception :
    @Bean
    @Qualifier("exceptionThrowingWriter")
    public ItemWriter<Transaction2> exceptionThrowingWriter(
            @Qualifier("FaultToleranceWriter") JdbcBatchItemWriter<Transaction2> delegate
    ) {
        log.info("Configuring ExceptionThrowingWriter wrapper");
        return new ExceptionThrowingWriter(delegate);
    }

    /*@Qualifier("Processor") APIProcessor perfectProcessor1 ,
                            @Qualifier("Listener") MyItemProcessListener myItemProcessListener)*/
    @Bean("StepTest")
    public Step simpleStep(
            @Qualifier("APIProcessor") APIProcessor APIProcessor,
            @Qualifier("APIProcessListener") APIProcessListener APIProcessListener ,
            @Qualifier("CompositeItemProcessor") CompositeItemProcessor<Transaction2, Transaction2> compositeItemProcessor
            /*@Qualifier("exceptionThrowingWriter") ItemWriter<Transaction2> writer*/ )
           {
        return new StepBuilder("simpleCsvToDbStep", jobRepository)
                .<Transaction2, Transaction2>chunk(10, transactionManager)

                .reader(simpleTransactionReader())
                .processor(compositeItemProcessor)
                .writer(simpleTransactionWriter())
                .listener(APIProcessListener)
                //.faultTolerant()
                //.skip(CanNotProcessItemException.class)
                 //.skipLimit(2)
                .listener(new LoggingSkipListener())
                // .processorNonTransactional()
                //.noRollback(CanNotProcessItemException.class)
                //
                //.listener(new LoggingItemWriteListener())

                .listener(new StepExecutionListener(){
                    @Override
                    public void beforeStep(StepExecution stepExecution) {
                        APIProcessor.setStepExecution(stepExecution);
                        APIProcessListener.setStepExecution(stepExecution);

                    }
                })
                .build();
    }


    @Bean("CompositeItemProcessor")
    public CompositeItemProcessor<Transaction2, Transaction2> compositeItemProcessor(
            @Qualifier("APIProcessor") ItemProcessor<Transaction2, Transaction2> APIProcessor
    ) {

        List<ItemProcessor<Transaction2, Transaction2>> delegates = new ArrayList<>(2);

        delegates.add(new FaultTolerProcess());
        delegates.add(APIProcessor);
        CompositeItemProcessor<Transaction2, Transaction2> processor = new CompositeItemProcessor<>();

        processor.setDelegates(delegates);


        return processor;
    }



    @Bean("APIProcessListener")
    APIProcessListener getListener() {

        return new APIProcessListener();
    }

    @Bean("APIProcessor")
    APIProcessor perfectProcessor() {
        return new APIProcessor();
    }


    @Bean
    @ConditionalOnProperty(name="app.job.name", havingValue="FaultTolerance")
    public Job simpleCsvImportJob(@Qualifier("StepTest") Step step) {
        log.info("Configuring simpleCsvImportJob");
        return new JobBuilder("eelommkkmlm", jobRepository)
                //.incrementer(new RunIdIncrementer())
                .start(step)
                .listener(new CustomJobExecutionListener())
                .build();
    }














}
