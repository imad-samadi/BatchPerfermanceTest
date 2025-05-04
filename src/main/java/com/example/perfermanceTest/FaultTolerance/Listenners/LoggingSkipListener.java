package com.example.perfermanceTest.FaultTolerance.Listenners;

import com.example.perfermanceTest.FaultTolerance.Transaction2;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;

@Slf4j

public class LoggingSkipListener implements SkipListener<Object, Object> {

    @Override
    public void onSkipInRead(Throwable t) {

        log.error("Item skipped during processing Reason: {}",t.getLocalizedMessage());
    }

    @Override
    public void onSkipInProcess(Object item, Throwable t) {
        Transaction2 t2 = (Transaction2) item;
        log.error("Item skipped during processing:{}Reason: {}", t2.getId(), t.getLocalizedMessage());
    }

    @Override
    public void onSkipInWrite(Object item, Throwable t) {
        Transaction2 t2 = (Transaction2) item;
        log.error("Item skipped during writing:{}Reason: {}", t2.getId(), t.getLocalizedMessage());
    }
}
