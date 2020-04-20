package com.dobest.kafka;

import com.dobest.kafka.executor.RecordState;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author :hujiansong
 * @date :2019/11/18 11:04
 * @since :1.8
 */
public abstract class AbsRecordContext<T> implements RecordContext<T> {

    private List<Map<String, Object>> processVal = new ArrayList<>();

    /**
     * handler thread name
     */
    @Getter
    @Setter
    protected String handlerThreadName;

    @Getter
    protected RecordState recordState = RecordState.NEW;

    @Override
    public void putAllProcessVal(List<Map<String, Object>> vals) {
        processVal.addAll(vals);
    }

    @Override
    public List<Map<String, Object>> getProcessVal() {
        return this.processVal;
    }

    @Override
    public void setHandlerName(String handlerThreadName) {
        this.recordState = RecordState.RUNNING;
        this.handlerThreadName = handlerThreadName;
    }

    @Override
    public void clearHandlerName() {
        // clear handlerThreadName
        this.recordState = RecordState.ACKED;
        this.handlerThreadName = null;
    }
}
