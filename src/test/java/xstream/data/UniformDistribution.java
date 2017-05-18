package xstream.data;

import java.util.Random;

public class UniformDistribution implements Distribution {
    double _min;
    double _max;
    private static final Random RNG = new Random();
    
    public UniformDistribution(double min, double max) {
        _min = min;
        _max = max;
    }

    @Override
    public double next() {
        double v = RNG.nextDouble();
        return v*(_max-_min) + _min;
    }

}
