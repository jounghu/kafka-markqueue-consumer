package com.dobest.kafka;

/**
 * @author shaohongliang
 * @since 2019/8/8 16:26
 */
public abstract class RecordProcessor<T>{
    private RecordProcessor next;

    public abstract void process(RecordContext<T> recordContext);

    public RecordProcessor nextProcessor() {
        return next;
    }

    public void setNextProcessor(RecordProcessor next) {
        this.next = next;
    }
}
