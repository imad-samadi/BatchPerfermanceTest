package com.example.perfermanceTest;

import com.example.perfermanceTest.Model.Transaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Slf4j

public class TransactionProcessor implements ItemProcessor<Transaction, Transaction> {

    @Override
    public Transaction process(final Transaction transaction) throws Exception {

         //log.info("Processing transaction: {}", transaction.getId());


             Thread.sleep(5); // Simulate 1ms processing time if needed


        return transaction;
    }
}
