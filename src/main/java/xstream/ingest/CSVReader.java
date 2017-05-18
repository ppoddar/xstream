package xstream.ingest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Observable;

import xstream.WritableTimeSeries;
import xstream.util.StopWatch;

/**
 * reads a CSV file and stores the data to a timeseries.
 * 
 * @author pinaki poddar
 *
 */
public class CSVReader extends Observable implements Runnable {
    private final File _file;
    private final WritableTimeSeries _series;
    
    private DateFormat _dateFormat;
    private int _headerLines;
    private int _timstampFieldIndex;
    
    /**
     * 
     * @param file the input file
     * @param series a readable timeseries 
     */
    public CSVReader(File file, WritableTimeSeries series) {
        _file = file;
        _series= series;
        _series.setTimeOrderStrict(false);
        _series.setAutoSortEvent(true);
    }
    
    public void run()  {
        ensureSingleThreaded();
        int eventLoaded = 0;
        int i = 0;
        StopWatch watch = new StopWatch();
        System.err.println("Loading " + _file + " ...");
        try {
            BufferedReader reader = new BufferedReader(new FileReader(_file));
            String line = null;
                while ((line = reader.readLine()) != null) {
                    i++;
                if (i <= _headerLines) continue;
                if (line.startsWith("#")) continue;
                String[] values = line.split(",");
                Date date = new Date();
                String timeString = values[_timstampFieldIndex].trim();
                try {
                    date = _dateFormat.parse(timeString);
                } catch (NumberFormatException ex) {
                    setChanged();
                    notifyObservers(new LoadError("Line " + i + " time string [" 
                         + timeString + "]" + " format " + _dateFormat, ex));
                    continue;
                }
                long ts = date.getTime();
                _series.write(ts, (Object[])values);
                eventLoaded++;
           }
            reader.close();
        } catch (ParseException ex) {
            setChanged();
            notifyObservers(new LoadError("error at Line " + i + "of " + _file, ex));
        } catch (IOException ex) {
            setChanged();
            notifyObservers(new LoadError("error at Line " + i + "of " + _file, ex));
        } finally {
            _series.close();
        }
        setChanged();
        long timeTaken = watch.stop();
        notifyObservers(new LoadFinshed(_file, eventLoaded, timeTaken));
    }
    
    

    
    public CSVReader withDateFormat(String dateFormat) {
        _dateFormat = new SimpleDateFormat(dateFormat);
        return this;
    }
    
    public CSVReader withHeaderLines(int n) {
        _headerLines = n;
        return this;
        
    }
    
    public CSVReader withTimestampFieldIndex(int n) {
        _timstampFieldIndex = n;
        return this;
    }
    
    /**
     * Ensures that run method is called from same thread.
     */
    Thread callingThread = null;
    void ensureSingleThreaded() {
        if (callingThread == null) {
            callingThread = Thread.currentThread();
        } else {
            if (callingThread != Thread.currentThread()) {
                throw new RuntimeException("called from multiple threads"
                        + " original thread " + callingThread
                        + " calling thread " + Thread.currentThread());
            }
        }
        
    }
    
    
    public static class LoadMessage {
        String message;
        File file;
    }
    
    public static class LoadStarted  extends LoadMessage {
        public LoadStarted(File f) {
            this.file = f;
        }
        
        public String toString() {
            return "Started loading " + file;
        }
    }
    
    public static class LoadFinshed  extends LoadMessage {
        int eventCount;
        long timeTaken;
        public LoadFinshed(File f, int n, long t) {
            this.file = f;
            this.eventCount = n;
            this.timeTaken = t;
        }
        
        public String toString() {
            return "Loaded " + file + " (" + eventCount + " events in "
                + timeTaken + " ms)";
        }
    }
    
    public static class LoadError extends LoadMessage {
        public final Exception ex;
        LoadError(String msg, Exception ex) {
            this.message = msg;
            this.ex = ex;
        }
        public String toString() {
            StringWriter writer = new StringWriter();
            writer.write(message + "\r\n\t");
            ex.printStackTrace(new PrintWriter(writer));
            return writer.toString();
        }
    }

}
