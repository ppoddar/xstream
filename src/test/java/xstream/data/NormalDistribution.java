package xstream.data;

import java.util.Random;

public class NormalDistribution implements Distribution {
    double _mean;
    double _stdDev;
    
    private static final Random RNG = new Random();
    public NormalDistribution(double mean, double stdDev) {
        _mean = mean;
        _stdDev = stdDev;
    }

    @Override
    public double next() {
        double v = RNG.nextGaussian();
        return v / _stdDev + _mean;
    }

}
