package com.example.perfermanceTest.FaultTolerance.Test;


import com.example.perfermanceTest.FaultTolerance.Transaction2;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;

@Slf4j
@Setter
public class APIProcessor implements ItemProcessor<Transaction2, Transaction2> {



    private StepExecution stepExecution;

    @Override
    public Transaction2 process(Transaction2 transaction2) throws Exception {

        List<Long> processedIds = (List<Long>) stepExecution.getExecutionContext().get("processedIds");


        if (processedIds != null && processedIds.contains(Long.valueOf(transaction2.getId()))) {
            log.info("Item :{} already been processed", transaction2.getId());
            return transaction2;
        }


        return doProcessing(transaction2);
    }

    private Transaction2 doProcessing(Transaction2 transaction2) {

        log.info("processing from API processing : {}", transaction2.getId());



        return transaction2;
    }


}

