package com.example.perfermanceTest.singleThread;

import com.example.perfermanceTest.Model.Transaction;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
/**
 * Helper class to build a JdbcPagingItemReader for Transaction objects.
 * Encapsulates DataSource, query provider and page size.
 */
@RequiredArgsConstructor
public class TransactionPagingReader {

    private final DataSource dataSource;
    private final PagingQueryProvider queryProvider;
    private final int pageSize;

    /**
     * Constructs and configures the JdbcPagingItemReader.
     * @return configured reader instance
     */
    public JdbcPagingItemReader<Transaction> build() {
        JdbcPagingItemReader<Transaction> reader = new JdbcPagingItemReader<>();
        reader.setName("JdbcPagingItemReader");
        reader.setDataSource(dataSource);
        reader.setQueryProvider(queryProvider);
        reader.setRowMapper(new BeanPropertyRowMapper<>(Transaction.class));
        reader.setPageSize(pageSize);
        return reader;
    }
}
