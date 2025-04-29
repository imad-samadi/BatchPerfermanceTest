package com.example.perfermanceTest.singleThread;

import com.example.perfermanceTest.BatchProperties;
import com.example.perfermanceTest.Listeners.SimpleChunkListener;
import com.example.perfermanceTest.Listeners.SimpleStepTimingListener;
import com.example.perfermanceTest.Model.Transaction;
import com.example.perfermanceTest.Model.TransactionProcessor;
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



/**
 * Configuration for a single-threaded Spring Batch job
 * that reads transactions from a database and writes them to a CSV file.
 */
@Configuration
@RequiredArgsConstructor
public class DbToCSVConfig {

    // DataSource providing connections to the database
    private final DataSource dataSource;
    // Repository for storing batch metadata (executions, steps)
    private final JobRepository jobRepository;
    // Transaction manager for handling chunk transactions
    private final PlatformTransactionManager transactionManager;
    // Externalized batch properties (e.g. chunk size, page size, output file path)
    private final BatchProperties batchProperties;

    /**
     * Factory bean for generating paging queries against the transactions table.
     * @return a configured SqlPagingQueryProviderFactoryBean
     */
    @Bean(name = "SqlPagingQueryProvider")
    public SqlPagingQueryProviderFactoryBean getQueryProvider3() {
        SqlPagingQueryProviderFactoryBean queryProvider = new SqlPagingQueryProviderFactoryBean();

        // Select all relevant columns for export
        queryProvider.setSelectClause("id, transaction_date, amount, created_at");
        queryProvider.setFromClause("FROM transactions");
        queryProvider.setDataSource(dataSource);

        // Define sorting to ensure consistent paging by ID ascending
        Map<String, Order> sortKeys = new LinkedHashMap<>();
        sortKeys.put("id", Order.ASCENDING);
        queryProvider.setSortKeys(sortKeys);

        return queryProvider;
    }

    /**
     * ItemReader that pages through the transactions table using JDBC.
     * Builds a JdbcPagingItemReader via a helper class.
     */
    @Bean(name = "PagingReader")
    public JdbcPagingItemReader<Transaction> pagingReader(
            DataSource dataSource,
            @Qualifier("SqlPagingQueryProvider") PagingQueryProvider queryProvider,
            BatchProperties batchProperties
    ) {
        // Delegate to TransactionPagingReader for cleaner encapsulation
        return new TransactionPagingReader(dataSource, queryProvider, batchProperties.getPageSize())
                .build();
    }

    /**
     * ItemWriter that writes Transaction objects into a delimited CSV file.
     * Uses a helper class for configuration.
     */
    @Bean
    public FlatFileItemWriter<Transaction> transactionWriter(BatchProperties batchProperties) {
        return new TransactionFileWriter(batchProperties.getOutputFile())
                .build();
    }

    /**
     * Simple pass-through processor .
     */
    @Bean
    public ItemProcessor<Transaction, Transaction> transactionProcessor() {
        return new TransactionProcessor();
    }

    /**
     * Definition of the batch step: read -> process -> write with chunk transactions.
     */
    @Bean
    public Step simpleTransactionStep(
            @Qualifier("PagingReader") ItemReader<Transaction> transactionReader,
            ItemProcessor<Transaction, Transaction> transactionProcessor,
            ItemWriter<Transaction> transactionWriter,
            SimpleStepTimingListener stepTimingListener,
            SimpleChunkListener chunkListener) {

        return new StepBuilder("simpleTransactionStep", jobRepository)
                // Configure chunk size and transaction manager
                .<Transaction, Transaction>chunk(batchProperties.getChunkSize(), transactionManager)
                .reader(transactionReader)
                .processor(transactionProcessor)
                .writer(transactionWriter)
                // Register listeners for step/ chunk timing
                .listener(stepTimingListener)
                .listener(chunkListener)
                .build();
    }
}

