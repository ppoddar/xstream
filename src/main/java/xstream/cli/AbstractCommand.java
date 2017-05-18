package xstream.cli;

import java.util.HashMap;
import java.util.Map;

public abstract class AbstractCommand implements Command {
    String _name;
    String[] _args;
    Map<String,String> _options = new HashMap<>();
    
    public AbstractCommand(String name) {
        _name = name;
    }

    @Override
    public String name() {
        return _name;
    }
    
    @Override
    public int length() {
        return 1;
    }
    
    @Override
    public void setOption(String option, String value) {
        _options.put(option,value);
    }
    
    @Override
    public String getOption(String option) {
        return _options.get(option);
    }
    
    @Override
    public void setArguments(String[] args) {
        _args = args;
    }
    
    @Override
    public String getArgument(int idx) {
        return _args[idx];
    }
    
    

    @Override
    public boolean accepts(String line) {
        return line != null && line.startsWith(name());
    }

    public String getUsage() {
        return "";
    }
    
    public String getDescription() {
        return "";
        
    }
}
