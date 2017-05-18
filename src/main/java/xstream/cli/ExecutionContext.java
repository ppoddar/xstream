package xstream.cli;

import java.util.Set;

import org.apache.spark.sql.SparkSession;

public interface ExecutionContext {
    Command findCommand(String signature);
    Set<String> getCommandNames();
    void write(String msg);
    void error(String msg, Exception ex);
    void stop();
    
    SparkSession getSession();
}
