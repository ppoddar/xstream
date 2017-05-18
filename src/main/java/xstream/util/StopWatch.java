package xstream.util;

public class StopWatch {
    private long _startTime;
    public StopWatch() {
        // TODO Auto-generated constructor stub
    }
    
    public void start() {
        _startTime = System.currentTimeMillis();
    }
    
    public long stop() {
        return System.currentTimeMillis() - _startTime;
    }

}
