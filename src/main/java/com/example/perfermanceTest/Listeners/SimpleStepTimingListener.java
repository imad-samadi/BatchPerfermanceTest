package com.example.perfermanceTest.Listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SimpleStepTimingListener implements StepExecutionListener {

    private ThreadLocal<Long> startTime = new ThreadLocal<>(); // Use ThreadLocal for concurrent steps later

    @Override
    public void beforeStep(StepExecution stepExecution) {
        startTime.set(System.currentTimeMillis());
        log.info("----> Step {} starting.", stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        long duration = System.currentTimeMillis() - startTime.get();
        log.info("<---- Step {} finished with status {}. ReadCount={}, WriteCount={}, CommitCount={}, Duration={} ms",
                stepExecution.getStepName(),
                stepExecution.getStatus(),
                stepExecution.getReadCount(),
                stepExecution.getWriteCount(),
                stepExecution.getCommitCount(), // Number of chunks committed
                duration);
        startTime.remove(); // Clean up ThreadLocal
        return stepExecution.getExitStatus(); // Don't change the exit status
    }
}
