package com.example.perfermanceTest.Listeners;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.stereotype.Component;
import org.springframework.batch.core.JobExecution;

@Component
@Slf4j
public class SimpleJobTimingListener  implements JobExecutionListener {


    private long startTime;

    @Override
    public void beforeJob(JobExecution  jobExecution) {
        startTime = System.currentTimeMillis();
        log.info("{} starting.", jobExecution.getJobInstance().getJobName());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        long duration = System.currentTimeMillis() - startTime;
        log.info("{} finished with status {}. Total duration: {} ms",
                jobExecution.getJobInstance().getJobName(),
                jobExecution.getStatus(),
                duration);
    }


}
