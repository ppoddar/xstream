package xstream.spark;

import java.io.Serializable;

import scala.runtime.AbstractFunction1;

/**
 * A function with two generic parameters, one input another for output.
 * 
 * @author pinaki poddar
 *
 * @param <T> type of input argument
 * @param <R> type of output argument
 */
@SuppressWarnings("serial")
public abstract class F1<T,R> extends AbstractFunction1<T, R> 
    implements Serializable {
//    @Override
//    public R apply(T arg0) {
//        // TODO Auto-generated method stub
//        return null;
//    }

}
