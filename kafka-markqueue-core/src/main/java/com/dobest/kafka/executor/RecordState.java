package com.dobest.kafka.executor;

/**
 * 2020/4/20 11:18
 *
 * @author hujiansong@dobest.com
 * @since 1.8
 */
public enum RecordState {
    NEW,
    RUNNING,
    ACKED,
    EMPTY;

    public static boolean running(RecordState state){
        return state.equals(RUNNING);
    }
}
