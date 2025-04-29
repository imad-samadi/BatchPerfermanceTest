package com.example.perfermanceTest.partition;

import com.example.perfermanceTest.BatchProperties;
import com.example.perfermanceTest.Model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for the worker side of a partitioned step.
 * Defines step-scoped reader and writer beans for each partition.
 */
@Configuration
@Slf4j
public class PartitionWorkerConfig {

    /**
     * Creates a PagingQueryProvider for a specific partition range.
     * This bean is step-scoped so each partition gets its own provider instance.
     *
     * @param dataSource       injected DataSource for database access
     * @param minId            minimum ID value for this partition (from ExecutionContext)
     * @param maxId            maximum ID value for this partition (from ExecutionContext)
     * @return configured PagingQueryProvider with dynamic WHERE clause
     */
    @Bean
    @StepScope
    @Qualifier("partitionQueryProvider")
    public PagingQueryProvider partitionQueryProvider(
            DataSource dataSource,
            @Value("#{stepExecutionContext['minId']}") Integer minId,
            @Value("#{stepExecutionContext['maxId']}") Integer maxId
    ) throws Exception {
        log.debug("Creating query provider for partition range [{}, {}]", minId, maxId);

        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
        factory.setDataSource(dataSource);
        factory.setSelectClause("SELECT id, transaction_date, amount, created_at");
        factory.setFromClause("FROM transactions");
        factory.setWhereClause("WHERE id BETWEEN " + minId + " AND " + maxId);

        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);
        factory.setSortKeys(sortKeys);

        // Return the PagingQueryProvider built by the factory
        return factory.getObject();
    }

    /**
     * Step-scoped JdbcPagingItemReader for a specific partition.
     * Each partition creates its own reader instance configured with its ID range.
     *
     * @param dataSource         injected DataSource for reading
     * @param queryProvider      the partition-specific PagingQueryProvider
     * @param batchProperties    holds pageSize configuration
     * @param partitionName      name of the partition (for logging and unique reader name)
     * @return configured JdbcPagingItemReader<Transaction>
     */
    @Bean(name = "PagingReaderPartition")
    @StepScope
    public JdbcPagingItemReader<Transaction> partitionedReader(
            DataSource dataSource,
            @Qualifier("partitionQueryProvider") PagingQueryProvider queryProvider,
            BatchProperties batchProperties,
            @Value("#{stepExecutionContext['partitionName']}") String partitionName
    ) {
        log.debug("Delegating creation of reader for {}", partitionName);
        return new PartitionedTransactionReader(
                dataSource,
                queryProvider,
                batchProperties,
                partitionName
        ).build();
    }

    /**
     * Step-scoped FlatFileItemWriter for a specific partition.
     * Writes output to a uniquely named CSV file per partition.
     *
     * @param batchProperties    holds the base output file path
     * @param partitionName      partition identifier (used to suffix the file name)
     * @return configured FlatFileItemWriter<Transaction>
     */
    @Bean
    @StepScope
    @Qualifier("partitionedWriter")
    public FlatFileItemWriter<Transaction> partitionedWriter(
            BatchProperties batchProperties,
            @Value("#{stepExecutionContext['partitionName']}") String partitionName
    ) {
        String outputFilePath =
                batchProperties.getOutputFile().replace(".csv", "-" + partitionName + ".csv");
        log.debug("Delegating creation of writer for {}", partitionName);
        return new PartitionedTransactionWriter(outputFilePath)
                .build();
    }
}
