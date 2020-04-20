package com.dobest.kafka;

import com.dobest.kafka.reject.RejectHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * @author shaohongliang
 * @since 2019/8/6 15:14
 */
@Slf4j
public class RecordHandler<T> implements Runnable {
    private RecordContext<T> recordContext;

    private RecordProcessorChain recordProcessorChain;

    private RejectHandler rejectHandler;

    private int retryNums = 0;


    public RecordHandler(RecordProcessorChain recordProcessorChain, RecordContext<T> recordContext, int retryNums, RejectHandler rejectHandler) {
        this.retryNums = retryNums;
        this.rejectHandler = rejectHandler;
        this.recordContext = recordContext;
        this.recordProcessorChain = recordProcessorChain;
    }


    @Override
    public void run() {
        this.recordContext.setHandlerName(Thread.currentThread().getName());
        RecordProcessor processor = recordProcessorChain.firstProcessor();
        if (recordContext.value() != null) {
            while (processor != null) {
                boolean finished = false;
                int retryNum = retryNums;
                while (!finished) {
                    try {
                        processor.process(recordContext);
                        finished = true;
                    } catch (Exception e) {
                        retryNum--;
                        log.error("process error, will wait 30s...", e);
                        try {
                            Thread.sleep(30000);
                        } catch (InterruptedException e1) {
                        }
                    }
                    //
                    if (!finished && retryNum <= 0) {
                        // start do reject
                        finished = rejectHandler.reject(recordContext);
                    }
                }

                if (recordContext.isAcked()) {
                    log.info("{} ack, topic: {}, partition: {}, offset: {}", Thread.currentThread().getName(), recordContext.getConsumerRecord().topic(), recordContext.getConsumerRecord().partition(), recordContext.getConsumerRecord().offset());
                    return;
                }
                processor = processor.nextProcessor();
            }
        } // 标记完成，等待异步提交
        recordContext.ack();
        this.recordContext.clearHandlerName();
        log.info("{} ack, topic: {}, partition: {}, offset: {}", Thread.currentThread().getName(), recordContext.getConsumerRecord().topic(), recordContext.getConsumerRecord().partition(), recordContext.getConsumerRecord().offset());

    }

    public void setRecordContext(RecordContext<T> context) {
        recordContext = context;
    }
}