package ppdz1;

import java.util.concurrent.atomic.DoubleAdder;

public class Integral {

    public static final int N = 100_000_000;
    public static final int THREADS = 6;
    public static final int STEPS_PER_THREAD = N / THREADS;

    public static final double A = 0.0;
    public static final double B = 10.0;
    public static final double DX = (B - A) / N;

    public static double f(double x) {
        return Math.sin(x) * Math.cos(x)
                * Math.sqrt(Math.abs(x) + 1.0)
                * Math.log(x * x + 1.0)
                * Math.exp(-x * 0.01);

    }

    static class Acc {
        volatile double acc = 0;
        synchronized public void addToAcc(double v) {
            acc += v;
        }
    }

    public static Thread taskThread(int n, double[] results) {
        return new Thread(() -> {
            int start  = n * STEPS_PER_THREAD;
            int finish = start + STEPS_PER_THREAD;
            double localSum = 0;
            for (int i = start; i < finish; i++) {
                double x = A + (i + 0.5) * DX;   // середина отрезка
                localSum += f(x) * DX;
            }
            results[n] = localSum;
        });
    }

    public static Thread taskMonitorThread(int n, Acc acc) {
        return new Thread(() -> {
            int start  = n * STEPS_PER_THREAD;
            int finish = start + STEPS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                double x = A + (i + 0.5) * DX;
                acc.addToAcc(f(x) * DX);
            }
        });
    }

    public static Thread taskAtomicThread(int n, DoubleAdder acc) {
        return new Thread(() -> {
            int start  = n * STEPS_PER_THREAD;
            int finish = start + STEPS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                double x = A + (i + 0.5) * DX;
                acc.add(f(x) * DX);
            }
        });
    }

    public static void measureSequential() {
        long start = System.nanoTime();
        double acc = 0;
        for (int i = 0; i < N; i++) {
            double x = A + (i + 0.5) * DX;
            acc += f(x) * DX;
        }
        long finish = System.nanoTime();
        System.out.println("Sequential result: " + acc);
        System.out.println("Sequential time (ms): " + (double)(finish - start) / 1_000_000);
        System.out.println();
    }

    public static void measureP() throws InterruptedException {
        double[] results = new double[THREADS];
        Thread[] threads = new Thread[THREADS];

        long t0 = System.nanoTime();
        for (int i = 0; i < THREADS; i++)
            threads[i] = taskThread(i, results);
        for (int i = 0; i < THREADS; i++)
            threads[i].start();
        for (int i = 0; i < THREADS; i++)
            threads[i].join();

        double res = 0;
        for (int i = 0; i < THREADS; i++)
            res += results[i];
        long t1 = System.nanoTime();

        System.out.println("Parallel result: " + res);
        System.out.println("Parallel time (ms): " + (double)(t1 - t0) / 1_000_000);
        System.out.println();
    }

    public static void measureAtomic() throws InterruptedException {
        DoubleAdder acc = new DoubleAdder();
        Thread[] threads = new Thread[THREADS];

        long t0 = System.nanoTime();
        for (int i = 0; i < THREADS; i++)
            threads[i] = taskAtomicThread(i, acc);
        for (int i = 0; i < THREADS; i++)
            threads[i].start();
        for (int i = 0; i < THREADS; i++)
            threads[i].join();
        long t1 = System.nanoTime();

        System.out.println("Atomic result: " + acc.sum());
        System.out.println("Atomic time (ms): " + (double)(t1 - t0) / 1_000_000);
        System.out.println();
    }

    public static void measureMon() throws InterruptedException {
        Acc acc = new Acc();
        Thread[] threads = new Thread[THREADS];

        long t0 = System.nanoTime();
        for (int i = 0; i < THREADS; i++)
            threads[i] = taskMonitorThread(i, acc);
        for (int i = 0; i < THREADS; i++)
            threads[i].start();
        for (int i = 0; i < THREADS; i++)
            threads[i].join();
        long t1 = System.nanoTime();

        System.out.println("Monitor result: " + acc.acc);
        System.out.println("Monitor time (ms): " + (double)(t1 - t0) / 1_000_000);
        System.out.println();
    }

    public static void main(String[] args) throws InterruptedException {
        measureSequential();
        measureP();
        measureAtomic();
        measureMon();
    }
}