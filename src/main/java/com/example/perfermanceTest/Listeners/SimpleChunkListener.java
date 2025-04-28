package com.example.perfermanceTest.Listeners; // Corrected package name

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class SimpleChunkListener implements ChunkListener {

    private ThreadLocal<Long> chunkStartTime = new ThreadLocal<>();
    private ThreadLocal<Integer> previousWriteCount = ThreadLocal.withInitial(() -> 0);

    @Override
    public void beforeChunk(ChunkContext context) {
        chunkStartTime.set(System.currentTimeMillis());
        previousWriteCount.set((int) context.getStepContext().getStepExecution().getWriteCount());
    }

    @Override
    public void afterChunk(ChunkContext context) {
        long duration = System.currentTimeMillis() - chunkStartTime.get();
        StepExecution stepExecution = context.getStepContext().getStepExecution();

        int itemsProcessedInChunk = (int) (stepExecution.getWriteCount() - previousWriteCount.get());

        log.debug("---- Chunk finished for step: {}. ItemsInChunk: {}. TotalWriteCount: {}. Duration: {} ms",
                context.getStepContext().getStepName(),
                itemsProcessedInChunk, // More accurate count for this chunk
                stepExecution.getWriteCount(), // Show total writes so far
                duration);
        // }

        chunkStartTime.remove();
        previousWriteCount.remove();
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        long duration = System.currentTimeMillis() - chunkStartTime.get();
        log.error("---- Chunk finished with ERROR for step: {}. Duration: {} ms. Error: {}",
                context.getStepContext().getStepName(),
                duration,
                context.getAttribute(ChunkListener.ROLLBACK_EXCEPTION_KEY));

        chunkStartTime.remove();
        previousWriteCount.remove();
    }
}
