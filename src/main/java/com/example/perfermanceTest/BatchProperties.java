package com.example.perfermanceTest;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

@Getter
@Setter
@NoArgsConstructor
@ToString
@Validated
@ConfigurationProperties(prefix = "batch")
public class BatchProperties {


    @Min(1)
    @Max(10_000)
    private int chunkSize =(10000);

    @Min(1)
    @Max(10_000)
    private int pageSize = 5000 ;

    @Min(1)
    @Max(10)
    private int corePoolSize = 5 ;

    @Min(1)
    @Max(10)
    private int partitionSize = 5;







    @Min(1)
    private int skipLimit = 10;

    /**
     * Default: 3, Maximum number of retry attempts as configured Retry policy, exceeding which fails
     * the job.
     */
    @Min(1)
    @Max(10)
    private int maxRetries = 3;

    /**
     * Default: 3s, Time duration to wait before the first retry attempt is made after a failure.
     */
    @NotNull
    private Duration backoffInitialDelay = Duration.ofSeconds(3);

    /** Default: 2, Factor by which the delay between consecutive retries is multiplied. */
    @Min(1)
    @Max(5)
    private int backoffMultiplier = 2;



    /**
     * Default: 8, Number of partitions that will be used to process the data concurrently. Should be
     * optimized as per available machine resources.
     */


    /**
     * Default: 100, Minimum number of records to trigger partitioning otherwise it could be counter
     * productive to do partitioning.
     */
    @Min(1)
    private int triggerPartitioningThreshold = 20;

    /**
     * Bean name of the Task Executor to be used for executing the jobs. By default
     * <code>SyncTaskExecutor</code> is used. Set to <code>applicationTaskExecutor</code> to use
     * <code>SimpleAsyncTaskExecutor</code> provided by Spring. Or use any other custom
     * <code>TaskExecutor</code> and set the bean name here.
     */
    private String taskExecutor;

    private String outputFile = "C:/Users/msi/Downloads/perfermanceTest/perfermanceTest/src/main/resources/transactions.csv" ;


}
