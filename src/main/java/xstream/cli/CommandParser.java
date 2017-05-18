package xstream.cli;

public class CommandParser {

    public CommandParser() {
    }
    
    void parse(Command command, String line) {
        String[] tokens = line.split(" ");
        int i = command.length();
        for (; i < tokens.length; i++) {
            String arg = tokens[i];
            if (arg.startsWith("-")) {
                command.setOption(arg, tokens[i+1]);
            } else {
                break;
            }
            
        }
        String[] args = new String[tokens.length - i];
        System.arraycopy(tokens, i, args, 0, args.length);
        command.setArguments(args);
        
    }

}
