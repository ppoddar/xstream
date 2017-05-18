package xstream;

/**
 * Defines a time range.
 * 
 * @author pinaki poddar
 *
 */

//TODO: documenation
class TimeRange {
    private long _start;
    private long _end;
    
    /**
     * Defines a range with start and end time.
     * 
     * @param start start time in UTC
     * @param end   end time in UTC. Must be greater than or equal to start.
     */
    public TimeRange(long start, long end) {
//        if (start < 0 || end < 0) {
//            throw new IllegalArgumentException("Start or end must be postive");
//        }
        if (start > end) {
            throw new IllegalArgumentException("Start " + start + " is "
                    + " after end " + end);
        }
        _start = start;
        _end = end;
    }
    
    boolean isValid() {
        return _start >= 0 && _end >= 0;
    }
    
    TimeRange add(TimeRange other) {
        TimeRange added = null;
        if (isValid()) {
            if (other.isValid()) {
                added = new TimeRange(
                        Math.min(other._start, this._start),
                        Math.max(other._end, this._end));
            } else {
                added = this.copy();
            }
        }  else {
            if (other.isValid()) {
                added = other.copy();
            } else {
                throw new RuntimeException(" cannot add two invalid time ranges");
            }
        }
        return added;
                
    }
    
    TimeRange copy() {
        return new TimeRange(_start, _end);
    }
    
    /**
     * Gets start time.
     * @return start time
     */
    public long getStartTime() {
        return _start;
    }
    
    /**
     * Gets end time.
     * @return end time
     */
    public long getEndTime() {
        return _end;
    }
    
    public String toString() {
        return "["+ _start + ':' + _end  + "]";
    }

}
