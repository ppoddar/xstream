package xstream.ingest;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import xstream.TimeSeries;
import xstream.TimeSeriesBuilder;
import xstream.WritableTimeSeries;

/**
 * Uploads traffic data to a Oracle NoSQL TimeSeries.
 * 
 * 
 * 
 * @author pinaki poddar
 *
 */
public class Ingester implements Observer {
    private static TimeSeries _series;
    private static File[] _files;
    private int _loadedFileCount;
    private long _loadedEventCount;
    private long _elapsedTime;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("***ERROR: missing input");
            System.err.println("Usage: java " + Ingester.class.getName() + "<input dir> <series URL>\r\n" + "where \r\n"
                    + "<input dir> is a directory containing csv files\r\n" + "<series URL> is a URL of NoSQL series");
            System.exit(1);
        }
        File dir = new File(args[0]);
        if (!dir.exists()) {
            System.err.println("*** ERROR: directory " + args[0] + " not found");
            System.exit(1);
        }
        if (!dir.isDirectory()) {
            System.err.println("*** ERROR: " + args[0] + " is not a directory");
            System.exit(1);
        }

        String[] fieldDefs = { "status STRING", "avgMeasuredTime DOUBLE", "avgSpeed DOUBLE", "extID INTEGER",
                "medianMeasuredTime DOUBLE", "TIMESTAMP STRING", "vehicleCount INTEGER", "id INTEGER",
                "REPORT_ID INTEGER" };

        _files = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".csv");
            }
        });
        System.err.println("Found " + _files.length + " csv files in " + dir);

        String seriesURL = args[1];
        System.err.println("Loading data in " + seriesURL);

        _series = new TimeSeriesBuilder()
                .withSeriesURL(seriesURL)
                .withFieldDefinitions(fieldDefs)
                .getOrCreate(false);

        Ingester ingester = new Ingester();
        ingester.run();
        ingester.report();
    }

    /**
     * Runs multiple readers on separate threads and collects performance
     * statistics.
     */
    public void run() {
        long startTime = System.currentTimeMillis();
        ExecutorService threadPool = Executors.newCachedThreadPool();
        for (File f : _files) {
            WritableTimeSeries ws = (WritableTimeSeries)new TimeSeriesBuilder()
                    .withSeriesURL(_series.getURL().toString())
                    .getOrCreate(false);
            CSVReader loader = new CSVReader(f, ws);
            loader.withDateFormat("yyyy-MM-dd'T'HH:mm:ss")
            .withHeaderLines(1)
            .withTimestampFieldIndex(5);

            loader.addObserver(this);

            threadPool.submit(loader);
        }
        threadPool.shutdown();
        try {
            threadPool.awaitTermination(2, TimeUnit.MINUTES);
        } catch (InterruptedException ex) {
        } finally {
            _elapsedTime = System.currentTimeMillis() - startTime;
        }
    }

    @Override
    public void update(Observable o, Object msg) {
         if (CSVReader.LoadFinshed.class.isInstance(msg)) {
            CSVReader.LoadFinshed m = CSVReader.LoadFinshed.class.cast(msg);
            _loadedFileCount++;
            _loadedEventCount += m.eventCount;
            System.err.println("Finsihed loading " + m.file + " (" + m.eventCount + " events)" + " [" + _loadedFileCount
                    + " of " + _files.length + "]");

        } else {
            System.err.println(msg);
        }
    }

    public void report() {
        int ingestionRate = (int) ((_loadedEventCount * 1000) / _elapsedTime);
        System.err.println("\r\nLoaded " + _loadedEventCount + " events from " + _loadedFileCount + " files in "
                + _elapsedTime + " ms" + " ingestion rate=" + ingestionRate + " event/sec");
    }

}
