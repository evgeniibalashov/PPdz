package ppdz3;

import java.awt.image.BufferedImage;
import java.io.File;
import javax.imageio.ImageIO;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Mandelbrot {

    static final int WIDTH = 1200;
    static final int HEIGHT = 900;
    static final int MAX_ITER = 1000;

    static int mandelbrot(double cx, double cy) {
        double zx = 0, zy = 0;
        int i = 0;
        while (i < MAX_ITER) {
            if (zx * zx + zy * zy > 4.0) break;
            double zxNew = zx * zx - zy * zy + cx;
            double zyNew = 2 * zx * zy + cy;
            zx = zxNew;
            zy = zyNew;
            i++;
        }
        return i;
    }

    static class StripTask implements Runnable {
        final BufferedImage image;
        final int fromY, toY;
        final double xMin, xMax, yMin, yMax;

        StripTask(BufferedImage image, int fromY, int toY,
                  double xMin, double xMax, double yMin, double yMax) {
            this.image = image;
            this.fromY = fromY;
            this.toY = toY;
            this.xMin = xMin;
            this.xMax = xMax;
            this.yMin = yMin;
            this.yMax = yMax;
        }

        @Override
        public void run() {
            for (int y = fromY; y < toY; y++) {
                for (int x = 0; x < WIDTH; x++) {
                    double cx = xMin + (xMax - xMin) * x / WIDTH;
                    double cy = yMin + (yMax - yMin) * y / HEIGHT;

                    int iter = mandelbrot(cx, cy);

                    int rgb;
                    if (iter == MAX_ITER) {
                        rgb = 0x000000;
                    } else {
                        int r = (iter * 5) % 256;
                        int g = (iter * 9) % 256;
                        int b = (iter * 13) % 256;
                        rgb = (r << 16) | (g << 8) | b;
                    }
                    image.setRGB(x, y, rgb);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        double xMin = -2.5, xMax = 1.0;
        double yMin = -1.2, yMax = 1.2;

        BufferedImage image = new BufferedImage(WIDTH, HEIGHT, BufferedImage.TYPE_INT_RGB);

        int threads = Runtime.getRuntime().availableProcessors();
        System.out.println("Используем потоков: " + threads);

        ExecutorService pool = Executors.newFixedThreadPool(threads);

        int rowsPerTask = HEIGHT / threads;
        for (int t = 0; t < threads; t++) {
            int fromY = t * rowsPerTask;
            int toY = (t == threads - 1) ? HEIGHT : (t + 1) * rowsPerTask;
            pool.submit(new StripTask(image, fromY, toY, xMin, xMax, yMin, yMax));
        }

        pool.shutdown();
        pool.awaitTermination(1, TimeUnit.HOURS);

        File output = new File("src/ppdz3/mandelbrot.png");
        ImageIO.write(image, "png", output);
        System.out.println("Картинка сохранена: " + output.getAbsolutePath());
    }
}