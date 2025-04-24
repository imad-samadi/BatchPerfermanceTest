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
    // Store the write count from *before* the current chunk started processing
    private ThreadLocal<Integer> previousWriteCount = ThreadLocal.withInitial(() -> 0);

    @Override
    public void beforeChunk(ChunkContext context) {
        chunkStartTime.set(System.currentTimeMillis());
        // Store the current write count before this chunk begins
        previousWriteCount.set((int) context.getStepContext().getStepExecution().getWriteCount());
        // log.trace("Starting chunk..."); // Optional
    }

    @Override
    public void afterChunk(ChunkContext context) {
        long duration = System.currentTimeMillis() - chunkStartTime.get();
        StepExecution stepExecution = context.getStepContext().getStepExecution();

        // Calculate items processed in *this specific* chunk
        int itemsProcessedInChunk = (int) (stepExecution.getWriteCount() - previousWriteCount.get());

        // Log only if duration exceeds a threshold, or log periodically to reduce noise
        // if (duration > 50) { // Example: Log only chunks taking > 50ms
        log.debug("---- Chunk finished for step: {}. ItemsInChunk: {}. TotalWriteCount: {}. Duration: {} ms",
                context.getStepContext().getStepName(),
                itemsProcessedInChunk, // More accurate count for this chunk
                stepExecution.getWriteCount(), // Show total writes so far
                duration);
        // }

        // Clean up ThreadLocals
        chunkStartTime.remove();
        previousWriteCount.remove(); // Clean up after the chunk is done
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        long duration = System.currentTimeMillis() - chunkStartTime.get();
        log.error("---- Chunk finished with ERROR for step: {}. Duration: {} ms. Error: {}",
                context.getStepContext().getStepName(),
                duration,
                context.getAttribute(ChunkListener.ROLLBACK_EXCEPTION_KEY));

        // Clean up ThreadLocals even on error
        chunkStartTime.remove();
        previousWriteCount.remove();
    }
}
