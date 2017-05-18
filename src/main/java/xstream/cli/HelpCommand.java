package xstream.cli;

public class HelpCommand extends AbstractCommand {

    public HelpCommand() {
        super("help");
    }


    @Override
    public void execute(String line, ExecutionContext ctx) throws Exception {
        String[] tokens = line.split(" ");
        if (tokens.length == 1) {
            ctx.write("available commands are: \r\n\t" + ctx.getCommandNames());
        } else {
            Command command = ctx.findCommand(tokens[1]);
            if (command != null) {
                String desc = command.getDescription();
                ctx.write(desc);
            } else {
                ctx.error("Unknown command " + tokens[1], null);
            }
            
        }
        
    }

}
