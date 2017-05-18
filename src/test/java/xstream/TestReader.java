package xstream;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.junit.Test;

import xstream.util.NoSQLURL;

public class TestReader {
    static NoSQLURL storeURL = new NoSQLURL("nosql://localhost:5000/kvstore/");
    @Test
    public void testRead() {
        String seriesURL = storeURL.toString() + "traffic";
        ReadableTimeSeries series = new TimeSeriesBuilder()
                .withSeriesURL(seriesURL)
                .openForRead();
        
        Iterator<Event> events = series.read();
        Event prev = null;
        int i = 0;
        while (events.hasNext()) {
            i++;
            events.next();
            if (i%1000 == 0) System.err.print(".");
            if (i%50000 == 0) System.err.println();
        }
        System.err.println("\r\n" + i + " events in " + seriesURL);
    }

}
