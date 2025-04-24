package com.example.perfermanceTest.partition;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.example.perfermanceTest.BatchProperties;
import com.example.perfermanceTest.Model.Transaction;
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

@Configuration
@Slf4j
public class PartitionWorkerConfig { //contains the reder writer


    @Bean
    @StepScope // Crucial: Creates a new provider instance for each partition step
    @Qualifier("partitionQueryProvider")
    public PagingQueryProvider partitionQueryProvider(
            DataSource dataSource,
            @Value("#{stepExecutionContext['minId']}") Integer minId, // Inject minId from context
            @Value("#{stepExecutionContext['maxId']}") Integer maxId) throws Exception { // Inject maxId

        log.debug("Creating query provider for partition range [{}, {}]", minId, maxId);

        SqlPagingQueryProviderFactoryBean factory = new SqlPagingQueryProviderFactoryBean();
        factory.setDataSource(dataSource);
        factory.setSelectClause("SELECT id, transaction_date, amount, created_at");
        factory.setFromClause("FROM transactions");
        factory.setWhereClause("WHERE id BETWEEN " + minId + " AND " + maxId); // Dynamic WHERE clause!
        Map<String, Order> sortKeys = new HashMap<>(1);
        sortKeys.put("id", Order.ASCENDING);
        factory.setSortKeys(sortKeys);

        return factory.getObject();
    }

    // --- Step-Scoped Reader ---
    @Bean
    @StepScope // Crucial: Creates a new reader instance for each partition step
    @Qualifier("partitionedReader")
    public JdbcPagingItemReader<Transaction> partitionedReader(
            DataSource dataSource,
            @Qualifier("partitionQueryProvider") PagingQueryProvider queryProvider, // Inject step-scoped provider
            BatchProperties batchProperties,
            @Value("#{stepExecutionContext['partitionName']}") String partitionName) { // Inject partition name

        log.debug("Creating reader for {}", partitionName);
        return new JdbcPagingItemReaderBuilder<Transaction>()
                .name("partitionedReader-" + partitionName) // Give unique name per partition
                .dataSource(dataSource)
                .queryProvider(queryProvider) // Use the partition-specific provider
                .pageSize(batchProperties.getPageSize())
                .rowMapper(new BeanPropertyRowMapper<>(Transaction.class))
                .build();
    }

    // --- Step-Scoped Writer ---
    @Bean
    @StepScope // Crucial: Creates a new writer instance for each partition step
    @Qualifier("partitionedWriter")
    public FlatFileItemWriter<Transaction> partitionedWriter(
            BatchProperties batchProperties,
            @Value("#{stepExecutionContext['partitionName']}") String partitionName) { // Inject partition name

        String outputFilePath = batchProperties.getOutputFile().replace(".csv", "-" + partitionName + ".csv");
        log.debug("Creating writer for {} writing to file: {}", partitionName, outputFilePath);

        BeanWrapperFieldExtractor<Transaction> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"id", "transactionDate", "amount", "createdAt"});
        fieldExtractor.afterPropertiesSet();

        DelimitedLineAggregator<Transaction> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        return new FlatFileItemWriterBuilder<Transaction>()
                .name("partitionedWriter-" + partitionName) // Unique name
                .resource(new FileSystemResource(outputFilePath)) // Dynamic file path
                .lineAggregator(lineAggregator)
                .headerCallback(writer -> writer.write("id,transaction_date,amount,created_at"))
                .shouldDeleteIfExists(true)
                .build();
    }
}
