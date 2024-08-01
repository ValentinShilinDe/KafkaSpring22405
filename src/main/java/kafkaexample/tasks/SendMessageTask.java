package kafkaexample.tasks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import kafkaexample.engine.Producer;
import java.time.LocalDate;
import java.util.concurrent.ExecutionException;

@Component
public class SendMessageTask {
    private final Logger logger = LoggerFactory.getLogger(SendMessageTask.class);

    private final Producer producer;
    public  SendMessageTask(Producer producer) { this.producer = producer; }

    @Scheduled(fixedRateString = "1000")
    public void send() throws ExecutionException, InterruptedException {
        ListenableFuture<SendResult<String, String>> listFuture = this.producer.sendMessage("INPUT_DATA", "KEY", LocalDate.now().toString());
        SendResult<String, String> result = listFuture.get();
        logger.info(String.format("Producer:\ntopic: %s\noffset: %d\npartition: %d\nvalue size: %d",
                result.getRecordMetadata().topic(),
                result.getRecordMetadata().offset(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().serializedValueSize()));
    }

}
