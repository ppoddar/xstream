package xstream.cli;

import xstream.spark.TimeseriesRDD;

public class RDDCommand extends AbstractCommand {

    public RDDCommand() {
        super("status");
    }

    @Override
    public void execute(String line, ExecutionContext ctx) throws Exception {
        String timeseriesURL = getArgument(0);
        new TimeseriesRDD(null, timeseriesURL);
        
    }

}
