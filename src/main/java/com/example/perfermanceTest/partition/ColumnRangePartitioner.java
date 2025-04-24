package com.example.perfermanceTest.partition;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ColumnRangePartitioner implements Partitioner {

    private final JdbcOperations jdbcTemplate;
    private final String table;
    private final String column;

    public ColumnRangePartitioner(DataSource dataSource, String table, String column) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.table = table;
        this.column = column;
    }

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        // Query min and max IDs
        Integer min = jdbcTemplate.queryForObject("SELECT MIN(" + column + ") FROM " + table, Integer.class);
        Integer max = jdbcTemplate.queryForObject("SELECT MAX(" + column + ") FROM " + table, Integer.class);

        if (min == null || max == null) {
            log.warn("No data found in table {} or column {} is null, cannot partition.", table, column);

            Map<String, ExecutionContext> result = new HashMap<>();
            ExecutionContext context = new ExecutionContext();
            context.putInt("minId", 0);
            context.putInt("maxId", 0);
            context.putString("partitionName", "partition0");
            result.put("partition0", context);
            return result;


        }

        log.info("Partitioning column '{}' in table '{}'. Min ID: {}, Max ID: {}, Grid Size: {}", column, table, min, max, gridSize);

        long targetSize = (max - min + 1) / (long) gridSize + 1; // Calculate target items per partition
        Map<String, ExecutionContext> result = new HashMap<>();
        long number = 0;
        long start = min;
        long end = start + targetSize - 1;

        while (start <= max) {
            ExecutionContext value = new ExecutionContext();
            String partitionName = "partition" + number;
            result.put(partitionName, value);

            if (end >= max) {
                end = max;
            }

            value.putInt("minId", (int) start);
            value.putInt("maxId", (int) end);
            value.putString("partitionName", partitionName);

            log.debug("Created Partition: {} | minId={}, maxId={}", partitionName, start, end);

            start = end + 1;
            end += targetSize;
            number++;
        }

        log.info("Created {} partitions for range [{}, {}]", result.size(), min, max);
        return result;
    }
}
