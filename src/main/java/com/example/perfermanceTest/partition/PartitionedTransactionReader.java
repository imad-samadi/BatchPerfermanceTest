package com.example.perfermanceTest.partition;
import com.example.perfermanceTest.Model.Transaction;
import com.example.perfermanceTest.BatchProperties;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
public class PartitionedTransactionReader {

    private final DataSource dataSource;
    private final PagingQueryProvider queryProvider;
    private final int pageSize;
    private final String partitionName;

    public PartitionedTransactionReader(DataSource dataSource,
                                        PagingQueryProvider queryProvider,
                                        BatchProperties batchProperties,
                                        String partitionName) {
        this.dataSource = dataSource;
        this.queryProvider = queryProvider;
        this.pageSize = batchProperties.getPageSize();
        this.partitionName = partitionName;
    }

    /**
     * Builds and returns a configured JdbcPagingItemReader for this partition.
     */
    public JdbcPagingItemReader<Transaction> build() {
        return new JdbcPagingItemReaderBuilder<Transaction>()
                .name("partitionedReader-" + partitionName)
                .dataSource(dataSource)
                .queryProvider(queryProvider)
                .pageSize(pageSize)
                .rowMapper(new BeanPropertyRowMapper<>(Transaction.class))
                .build();
    }
}
