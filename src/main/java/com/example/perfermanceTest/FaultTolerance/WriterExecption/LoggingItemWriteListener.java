package com.example.perfermanceTest.FaultTolerance.WriterExecption;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.item.Chunk;

@Slf4j
public class LoggingItemWriteListener implements ItemWriteListener<Object> {

    @Override
    public void beforeWrite(Chunk<? extends Object> items) {
        log.info("Preparing to write {} items", items.getItems().size());
    }

    /**
     * Called after successfully writing a chunk of items.
     */
    @Override
    public void afterWrite(Chunk<? extends Object> items) {
        log.info("Successfully wrote {} items", items.getItems().size());
    }

    /**
     * Called when an error occurs during writing a chunk of items.
     * Logs the exception and each failed item.
     */
    @Override
    public void onWriteError(Exception exception, Chunk<? extends Object> items) {
        log.error("Error writing "+items.getItems().size()+ "items because of : "+exception.getMessage());
       /* for (Object item : items.getItems()) {
            log.error("Failed item: {}", item);
        }*/
    }
}
