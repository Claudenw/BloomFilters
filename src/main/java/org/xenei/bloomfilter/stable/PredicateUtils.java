package org.xenei.bloomfilter.stable;

import java.util.function.IntConsumer;
import java.util.function.IntPredicate;

public class PredicateUtils {
    
   private PredicateUtils() {}
   
   public static IntPredicate from(IntConsumer consumer, boolean result) {
       return i -> {consumer.accept(i);return result;};
   }

}
