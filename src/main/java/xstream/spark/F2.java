package xstream.spark;

import java.io.Serializable;

import scala.runtime.AbstractFunction2;

/**
 * A function with three generic parameters, two input and third for output.
 * 
 * @author pinaki poddar
 *
 * @param <T1> type of first input argument
 * @param <T2> type of second input argument
 * @param <R> type of output argument
 */
@SuppressWarnings("serial")
public abstract class F2<T1,T2, R> extends AbstractFunction2<T1, T2, R> 
    implements Serializable {
//    @Override
//    public R apply(T1 arg0, T2 arg1) {
//        // TODO Auto-generated method stub
//        return null;
//    }

}
