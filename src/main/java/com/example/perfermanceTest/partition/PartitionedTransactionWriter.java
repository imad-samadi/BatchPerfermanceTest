package com.example.perfermanceTest.partition;
import com.example.perfermanceTest.Model.Transaction;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.core.io.FileSystemResource;
public class PartitionedTransactionWriter {
    private final String outputFilePath;

    public PartitionedTransactionWriter(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }

    /**
     * Builds and returns a configured FlatFileItemWriter for this partition.
     */
    public FlatFileItemWriter<Transaction> build() {
        BeanWrapperFieldExtractor<Transaction> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"id", "transactionDate", "amount", "createdAt"});
        extractor.afterPropertiesSet();

        DelimitedLineAggregator<Transaction> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(extractor);

        return new FlatFileItemWriterBuilder<Transaction>()
                .name("partitionedWriter")
                .resource(new FileSystemResource(outputFilePath))
                .lineAggregator(aggregator)
                .headerCallback(writer -> writer.write("id,transaction_date,amount,created_at"))
                .shouldDeleteIfExists(true)

                .build();
    }
}
