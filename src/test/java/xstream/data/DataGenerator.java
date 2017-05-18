package xstream.data;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;

public class DataGenerator extends Observable {

    public DataGenerator() {
        // TODO Auto-generated constructor stub
    }
    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static NumberFormat numberFormat = new DecimalFormat("##.#");

    public static void main(String[] args) throws Exception {
        int NFile = 2;
        int NEventPerFile = 10;
        int interval = 5;
        String startDateString = "2017-05-02 00:00:00.000";
        File dir = new File(".");
        
        for (int i = 0; i < args.length-1; i++) {
            String arg = args[i];
            if (!arg.startsWith("-")) break;
            if ("-nFile".equals(arg)) 
                NFile = Integer.parseInt(args[++i]);
            if ("-nEventPerFile".equals(arg)) 
                NEventPerFile = Integer.parseInt(args[++i]);
            if ("-nInterval".equals(arg)) 
                interval = Integer.parseInt(args[++i]);
            if ("-dateFormat".equals(arg)) 
                dateFormat = new SimpleDateFormat(args[++i]);
            if ("-numberFormat".equals(arg)) 
                numberFormat = new DecimalFormat(args[++i]);
            if ("-startDate".equals(arg)) 
                startDateString = args[++i];
            
            if ("-outDir".equals(arg)) 
                dir = new File(args[++i]);
        }

        Date startDate = dateFormat.parse(startDateString);
        if (!dir.exists()) dir.mkdirs();

        Map<String, Distribution> dimensions = new HashMap<String, Distribution>();
        dimensions.put("x", new NormalDistribution(10, 2));
        dimensions.put("y", new UniformDistribution(20, 30));

        long start = startDate.getTime();
        long t = start;
        for (int i = 0; i < NFile; i++) {
            File file = new File(dir, 
                    MessageFormat.format("series-{0}", ""+i));
            PrintStream fout = new PrintStream(new FileOutputStream(file));
            System.err.println("writing to " + file);
            List<Double> data = new ArrayList<Double>();
            for (long j = 0; j <= NEventPerFile; j++) {
                data.add(1.0*t);
                t += interval;
                for (Distribution d : dimensions.values()) {
                    data.add(d.next());
                }
                writeCSV(fout, data);
                data.clear();
            }
            fout.close();
        };
        
    }
    
    static void writeCSV(PrintStream fout, List<Double> data) {
        int i = 0;
        for (double d : data) {
            String v = (i == 0) ? 
                    dateFormat.format(new Date((long)d)) 
                    : numberFormat.format(d);
            fout.print(v); fout.print(",");
            i++;
        }
        fout.println();
    }
    
    

}
