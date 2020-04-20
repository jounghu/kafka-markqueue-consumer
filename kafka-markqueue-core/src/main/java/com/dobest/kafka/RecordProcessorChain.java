package com.dobest.kafka;

import java.util.LinkedList;

/**
 * @author shaohongliang
 * @since 2019/9/11 16:04
 */
public class RecordProcessorChain {
    private LinkedList<RecordProcessor> recordProcessorList = new LinkedList<>();

    public RecordProcessorChain addProcessor(RecordProcessor recordProcessor){
        if(recordProcessorList.size() > 0){
            recordProcessorList.getLast().setNextProcessor(recordProcessor);
        }
        recordProcessorList.add(recordProcessor);
        return this;
    }

    public RecordProcessorChain addProcessorFirst(RecordProcessor recordProcessor){
        if(recordProcessorList.size() > 0){
            recordProcessor.setNextProcessor(recordProcessorList.getFirst());
        }
        recordProcessorList.addFirst(recordProcessor);
        return this;
    }

    public RecordProcessor firstProcessor() {
        return recordProcessorList.size() == 0 ? null : recordProcessorList.getFirst();
    }
}
