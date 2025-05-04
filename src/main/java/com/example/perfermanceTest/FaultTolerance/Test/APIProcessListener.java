package com.example.perfermanceTest.FaultTolerance.Test;


import com.example.perfermanceTest.FaultTolerance.Transaction2;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemProcessListener;
import org.springframework.batch.core.StepExecution;

import java.util.ArrayList;
import java.util.List;
@Slf4j
@Setter
public class APIProcessListener implements ItemProcessListener<Transaction2, Transaction2> {

    private StepExecution stepExecution;



    @Override
    public void afterProcess(Transaction2 item, Transaction2 result) {

        if (result != null) {
            List<Long> processedIds = (List<Long>) stepExecution.getExecutionContext().get("processedIds");
            if (processedIds == null) {
                processedIds = new ArrayList<>();
                stepExecution.getExecutionContext().put("processedIds", processedIds);
            }
            processedIds.add(Long.valueOf(item.getId()));
            //log.info("ADDED TO THE EXECUTION CONTEXT : {}", processedIds);
        }
    }
}

