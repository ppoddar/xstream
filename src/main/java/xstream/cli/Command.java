package xstream.cli;

public interface Command {
    /**
     * gets name of this command.
     * @return name of this command.
     */
    String name();
    
    /**
     * affirms if the given command line can be processed by this command.
     * @param line command line
     * @return true if this command is recognized
     */
    boolean accepts(String line);
    
    /**
     * sets value of an option. The command may not recognize this option.
     * @param option option key
     * @param value value specified in command line
     */
    void setOption(String option, String value);
    
    /**
     * Gets value of the option as set by command line.
     * @param option option key
     * @return value of the option
     */
    String getOption(String option);
    
    /**
     * no of tokens for matching the command and ignore them at parse.
     * @return number of tokens used in recognizing the command. mostly
     * one.
     */
    int length();
    
    /**
     * gets short description to use this command
     * @return a usage string
     */
    String getUsage();
    
    
    /**
     * gets detail description of function this command
     * @return a description string
     */
    String getDescription();
    
    /**
     * sets arguments to this command.
     * @param args arguments from command line. The tokens that follow the
     * options. 
     */
    void setArguments(String[] args);
    
    /**
     * gets argument at given index.
     * @param idx 0-based argument index 
     * @return value of an argument at given index
     */
    String getArgument(int idx);
    
    /**
     * executes this command
     * @param line entire command line
     * @param ctx context of execution. the command should write output
     * or error to this context.
     * @throws Exception any exception to execute this command or illegal/
     * insufficient input
     */
    void execute(String line, ExecutionContext ctx) throws Exception;

}
