package xstream.cli;

public class ExitCommand extends AbstractCommand {

    public ExitCommand() {
        super("exit");
    }

    @Override
    public void execute(String line, ExecutionContext ctx) throws Exception {
        ctx.stop();
        
    }

}
