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
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.batch.item.database.Order;
import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class DbToJSONConfig {

    private final DataSource dataSource;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final BatchProperties batchProperties;

    @Bean(name = "SqlPagingQueryProvider")
    public SqlPagingQueryProviderFactoryBean getQueryProvider3() {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();

        // Select all columns (or specify the ones you need)
        queryProvider.setSelectClause("id, transaction_date, amount, created_at");
        queryProvider.setFromClause("FROM transactions");
        queryProvider.setDataSource(dataSource);

        // Define sort keys (optional, but required for paging)
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("id", Order.ASCENDING); // Ensures consistent ordering for paging
        queryProvider.setSortKeys(sortKeys);

        return queryProvider;
    }
    @Bean(name = "PagingReader")
    public JdbcPagingItemReader<Transaction> pagingReadDb3(@Qualifier("SqlPagingQueryProvider") PagingQueryProvider queryProvider) { // Inject the query provider
        JdbcPagingItemReader<Transaction> reader = new JdbcPagingItemReader<>();
        reader.setName("JdbcPagingItemReader");
        reader.setDataSource(dataSource);
        reader.setQueryProvider(queryProvider);
        reader.setRowMapper(new BeanPropertyRowMapper<>(Transaction.class));
        reader.setPageSize(batchProperties.getPageSize());
        return reader;
    }

    @Bean
    public FlatFileItemWriter<Transaction> transactionWriter() {

        BeanWrapperFieldExtractor<Transaction> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"id", "transactionDate", "amount", "createdAt"});
        fieldExtractor.afterPropertiesSet();

        DelimitedLineAggregator<Transaction> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        return new FlatFileItemWriterBuilder<Transaction>()
                .name("transactionWriter")
                .resource(new FileSystemResource(batchProperties.getOutputFile()))
                .lineAggregator(lineAggregator)
                .headerCallback(writer -> writer.write("id,transaction_date,amount,created_at"))
                .shouldDeleteIfExists(true)
                .build();
    }
    @Bean
    ItemProcessor<Transaction,Transaction> transactionProcessor() {
        return new TransactionProcessor();
    }
    @Bean
    public Step simpleTransactionStep(

            @Qualifier("PagingReader") ItemReader<Transaction> transactionReader, // Use bean name from DbToJSONConfig
            ItemProcessor<Transaction, Transaction> transactionProcessor,      // Inject your processor bean
            ItemWriter<Transaction> transactionWriter,                      // Inject your writer bean
            SimpleStepTimingListener stepTimingListener,                      // Inject step listener
            SimpleChunkListener chunkListener) {                              // Inject chunk listener

        return new StepBuilder("simpleTransactionStep", jobRepository)
                .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), transactionManager) // Configure chunk size
                .reader(transactionReader)
                .processor(transactionProcessor)
                .writer(transactionWriter)
                .listener(stepTimingListener) // Register the step listener
                .listener(chunkListener)      // Register the chunk listener
                // .faultTolerant() // Add fault tolerance later if needed
                .build();
    }

}
