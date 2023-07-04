package org.xenei.bloomfilter.stable;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;

import org.apache.commons.collections4.bloomfilter.IndexProducer;

public class Test {

    public static class Thing implements IndexProducer {
        volatile int[] values;
        AtomicInteger counter = new AtomicInteger();

        Thing() {
            counter.set(0);
            values = new int[5];
            for (int i = 0; i < 5; i++) {
                values[i] = i;
            }
        }

        @Override
        public boolean forEachIndex(IntPredicate predicate) {
            int[] myValues = values;
            for (int value : myValues) {
                if (!predicate.test(value)) {
                    return false;
                }
            }
            return true;
        }

        
        public synchronized void merge(int... newValues) {
            int count = counter.getAndIncrement();
            int[] result = new int[5];
            int[] read = new int[5];
            int idx[] = { 0 };
            forEachIndex(i -> {
                read[idx[0]] = i;
                result[idx[0]] = newValues[idx[0]++] + i;
                return true;
            });
            values = result;
            System.out.format( "%s, Updated values [%s %s %s %s %s] + [ %s %s %s %s %s ] = [ %s %s %s %s %s ]\n",
                    count, read[0], read[1], read[2], read[3], read[4],
                    newValues[0], newValues[1], newValues[2], newValues[3], newValues[4],
                    result[0], result[1], result[2], result[3], result[4]
                    );
        }
    }

    private static class Reporter implements Runnable {
        String name;
        Thing t;

        Reporter(String name, Thing t) {
            this.name = name;
            this.t = t;
        }

        @Override
        public void run() {
            boolean running = true;
            for (int idx=0;idx<5;idx++) {
                try {
                    StringBuffer result = new StringBuffer(this.name);
                    t.forEachIndex(i -> {
                        result.append(String.format(" %s", i));
                        try {
                            Thread.sleep(5);
                        } catch (InterruptedException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        return true;
                    });
                    System.out.println(result);
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    running = false;
                }
            }
        }
    }

    private static class Incrementer implements Runnable {
        String name;
        int[] values;
        Thing t;

        Incrementer(String name, Thing t, int... values) {
            this.name = name;
            this.values = values;
            this.t = t;
        }

        @Override
        public void run() {
            boolean running = true;
            for (int idx=0;idx<5;idx++) {
                t.merge(values);
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Thing t = new Thing();
        ExecutorService service = Executors.newFixedThreadPool(7);

        service.execute(new Reporter("First", t));
        t.merge(1, 2, 3, 4, 5);
        service.execute(new Reporter("Second", t));
        service.execute(new Incrementer("+1", t, 1, 1, 1, 1, 1));
        service.execute(new Reporter("Third", t));
        service.execute(new Incrementer("-1", t, -1, -1, -1, -1, -1));
        service.execute(new Incrementer("+2", t, 2, 2, 2, 2, 2));
        service.execute(new Incrementer("-2", t, -2, -2, -2, -2, -2));
        Thread.sleep(TimeUnit.SECONDS.toMillis(2));
        service.shutdown();
        service.awaitTermination(2, TimeUnit.SECONDS);

    }
}
