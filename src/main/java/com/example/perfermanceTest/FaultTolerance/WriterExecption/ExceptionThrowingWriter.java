package com.example.perfermanceTest.FaultTolerance.WriterExecption;
import com.example.perfermanceTest.FaultTolerance.CanNotProcessItemException;
import com.example.perfermanceTest.FaultTolerance.Transaction2;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;

// this a fake writer to test if an exception happened during writing

@RequiredArgsConstructor
@Slf4j
public class ExceptionThrowingWriter implements ItemWriter<Transaction2> {

    private final JdbcBatchItemWriter<Transaction2> delegateWriter;

    private static final int TRIGGER_ID = 13;

    @Override
    public void write(Chunk<? extends Transaction2> chunk) throws Exception {
        //log.info("--> Wrapper Writer processing chunk of size: {}", chunk.getItems().size());

        boolean containsTriggerId = chunk.getItems().stream()
                .anyMatch(item -> item.getId().equals(TRIGGER_ID));

        if (containsTriggerId) {
            log.error("!!! Chunk contains Trigger ID {}. Throwing fake CanNotProcessItemException.", TRIGGER_ID);
            throw new CanNotProcessItemException("Simulated writer error triggered by item with ID " + TRIGGER_ID);
        } else {
            //log.info("--> No trigger ID found. Delegating write for {} items.", chunk.getItems().size());
            delegateWriter.write(chunk);
        }
    }
}
