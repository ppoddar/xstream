package xstream.example;

import xstream.TimeSeries;
import xstream.TimeSeriesBuilder;

public class CreateSeries {

    public static void main(String[] args) {
        String url = "nosql://localhost:5000/kvstore/traffic";
        TimeSeries series = new TimeSeriesBuilder()
        .withSeriesURL(url)
            .openForWrite();
        System.err.println(series + " contains " + series.size() + " events");
    }

}
