package com.example.perfermanceTest.singleThread;

import com.example.perfermanceTest.Model.Transaction;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;

/**
 * Helper class to build a FlatFileItemWriter for Transaction objects.
 * Encapsulates file path and CSV formatting details.
 */
@RequiredArgsConstructor
public class TransactionFileWriter {

    // Path to the output CSV file
    private final String outputFilePath;

    /**
     * Constructs and configures the FlatFileItemWriter.
     * @return configured writer instance
     */
    public FlatFileItemWriter<Transaction> build() {
        // Extract fields by property names in Transaction class
        BeanWrapperFieldExtractor<Transaction> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"id", "transactionDate", "amount", "createdAt"});
        fieldExtractor.afterPropertiesSet();

        // Use comma delimiter to aggregate fields
        DelimitedLineAggregator<Transaction> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        // Build the writer with header and overwrite behavior
        return new FlatFileItemWriterBuilder<Transaction>()
                .name("transactionWriter")
                .resource(new FileSystemResource(outputFilePath))
                .lineAggregator(lineAggregator)
                .headerCallback(writer -> writer.write("id,transaction_date,amount,created_at"))
                .shouldDeleteIfExists(true)
                .build();
    }
}

