package xstream.cli;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.SparkSession;

import jline.console.ConsoleReader;
import xstream.util.StringHelper;

public class CommandLineClient implements ExecutionContext {
    private SparkSession _session;
    
    private boolean _stopped;
    private Map<String, Command> _commands = new HashMap<String, Command>();
//    private BufferedReader _input;
//    private PrintWriter _output;
    private ConsoleReader _reader;
    private String _banner = "Command Line client";
    private String _continuation = "\\";
    private String _prompt = "> ";
    
    public static void main(String[] args) throws IOException {
        new CommandLineClient().run();
    }
    
    public CommandLineClient() throws IOException {
//        _input = new BufferedReader(System.console().reader());
//        _output = System.console().writer();
        _reader = new ConsoleReader();
        _reader.setPrompt(_prompt);
        register(new HelpCommand());
        register(new ExitCommand());
    }
    
    public void setSparkSession(SparkSession session) {
        _session = session;
    }
    
    public SparkSession getSession() {
        return _session;
    }
    
    void register(Command command) {
        _commands.put(command.name(), command);
    }
    
    public Set<String> getCommandNames() {
        return _commands.keySet();
    }
    
    public void run() {
        write(_banner);
        while (!isStopped()) {
            prompt();
            String line = readInput();
            if (StringHelper.isEmpty(line)) {
                continue;
            }
            Command command = findCommand(line);
            if (command != null) {
                try {
                    command.execute(line, this);
                } catch (Exception ex) {
                    error(line, ex);
                }
            } else {
                help(line);
            }
        }
    }
    
    void help(String line) {
        write("**WARN: can not understand command [" + line + "]");
        write("available commands are:");
        write("\t" + getCommandNames());
    }
    
    boolean isStopped() {
        return _stopped;
    }
    
    public void stop() {
        _stopped = true;
    }
    
    public Command findCommand(String line) {
        for (Map.Entry<String, Command> e : _commands.entrySet()) {
            if (e.getValue().accepts(line)) {
                return e.getValue();
            }
        }
        return null;
    }
    
    String readInput() {
        StringBuffer readBuffer = new StringBuffer();
        while (true) {
            try {
                // String input = _input.readLine();
                String input = _reader.readLine();
                if (StringHelper.isEmpty(input)) {
                    prompt();
                    continue;
                }
                readBuffer.append(trimLine(input));
                if (isContinuation(input)) {
                    prompt();
                    continue;
                } else {
                    break;
                }
            } catch (IOException ex) {
            
            }
        }
        return readBuffer.toString();
        
    }
    
    boolean isContinuation(String s) {
        return s.endsWith(_continuation);
    }
    
    String trimLine(String line) {
        return isContinuation(line) ? 
               line.substring(0, line.length()-_continuation.length())
               : line;
    }
    
    void prompt() {
//        try {
//            _reader.print(_prompt);
//            _reader.flush();
//        } catch (IOException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        _output.write(_prompt);
//        _output.flush();
    }
    
    public void write(String s) {
//        _output.println(s);
        try {
            _reader.println(s);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public void error(String s, Exception ex) {
        write(s);
//        if (ex != null) {
//            ex.printStackTrace(_output);
//        }
    }
    
}
