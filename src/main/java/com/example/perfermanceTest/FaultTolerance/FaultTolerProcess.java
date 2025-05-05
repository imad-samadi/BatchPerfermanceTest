package com.example.perfermanceTest.FaultTolerance;

import com.example.perfermanceTest.Model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

@Slf4j
public class FaultTolerProcess implements ItemProcessor<Transaction2, Transaction2> {
    @Override
    public Transaction2 process(Transaction2 item) throws Exception {
        log.info("processing from fault tolerance item  : {}", item.getId());

      /* if(item.getId().compareTo(13)==0){
            throw new CanNotProcessItemException("Can not process item with id 13");
        }*/
        return item;
    }
}

