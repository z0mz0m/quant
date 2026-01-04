package arrows;



import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A utility class to read and analyze trade data from an Arrow stream file (.arrows) using memory-mapped I/O,
 * the Java Vector API for calculations, and parallel sorting for improved performance.
 * NOTE: This class uses an incubator feature. The code must be compiled and run with:
 * --add-modules jdk.incubator.vector
 */
public class ArrowSTradeVectorReader {

    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

    /**
     * Analyzes an Arrow stream file to calculate the CDF of inter-trade arrival times.
     *
     * @param arrowFilePath The path to the Arrow stream file.
     */
    public static void analyzeTrades(String arrowFilePath) {
        long totalStartTime = System.nanoTime(); // Start the overall timer

        File arrowFile = new File(arrowFilePath);
        if (!arrowFile.exists()) {
            System.err.println("Error: File not found at " + arrowFilePath);
            return;
        }

        long setupEndTime;
        long readingStartTime;
        long readingEndTime;
        long conversionStartTime;
        long conversionEndTime;
        long sortStartTime;
        long sortEndTime;
        long totalEndTime;

        try (RootAllocator allocator = new RootAllocator()) {
            try (RandomAccessFile raf = new RandomAccessFile(arrowFile, "r")) {
                try (FileChannel fileChannel = raf.getChannel()) {
                    try (ArrowStreamReader reader = new ArrowStreamReader(fileChannel, allocator)) {

                        System.out.println("Successfully opened Arrow stream file: " + arrowFilePath);
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();
                        System.out.println("Schema: " + root.getSchema());
                        System.out.println("-----------------------------------------");
                        System.out.println("Calculating inter-trade arrival times using Vector API...");

                        setupEndTime = System.nanoTime();

                        List<Long> arrivalTimeDeltas = new ArrayList<>();
                        long lastTimestamp = -1;
                        long totalTrades = 0;

                        long[] deltaChunk = new long[SPECIES.length()];

                        // Add accumulators for more granular timing
                        long totalBatchLoadNanos = 0;
                        long totalMemCopyNanos = 0;
                        long totalVectorCalcNanos = 0;
                        long t1, t2;


                        readingStartTime = System.nanoTime();
                        long batchReadStart = System.nanoTime();
                        while (reader.loadNextBatch()) {
                            totalBatchLoadNanos += (System.nanoTime() - batchReadStart);

                            int batchSize = root.getRowCount();
                            if (batchSize == 0) continue;

                            BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp_millis_utc");

                            long[] batchTimestamps = new long[batchSize];

                            t1 = System.nanoTime();
                            java.nio.LongBuffer longBuffer = timestampVector.getDataBuffer().nioBuffer(0, batchSize * Long.BYTES).asLongBuffer();
                            longBuffer.get(batchTimestamps, 0, batchSize);
                            t2 = System.nanoTime();
                            totalMemCopyNanos += (t2 - t1);

                            t1 = System.nanoTime();
                            if (lastTimestamp != -1) {
                                arrivalTimeDeltas.add(batchTimestamps[0] - lastTimestamp);
                            }

                            int i = 1;
                            int loopBound = SPECIES.loopBound(batchSize);

                            for (; i < loopBound; i += SPECIES.length()) {
                                LongVector current = LongVector.fromArray(SPECIES, batchTimestamps, i);
                                LongVector previous = LongVector.fromArray(SPECIES, batchTimestamps, i - 1);
                                LongVector delta = current.sub(previous);
                                delta.intoArray(deltaChunk, 0);
                                for (long d : deltaChunk) {
                                    arrivalTimeDeltas.add(d);
                                }
                            }

                            for (; i < batchSize; i++) {
                                long delta = batchTimestamps[i] - batchTimestamps[i - 1];
                                arrivalTimeDeltas.add(delta);
                            }
                            t2 = System.nanoTime();
                            totalVectorCalcNanos += (t2 - t1);


                            lastTimestamp = batchTimestamps[batchSize - 1];
                            totalTrades += batchSize;
                            batchReadStart = System.nanoTime();
                        }
                        readingEndTime = System.nanoTime();

                        System.out.println("Finished reading " + totalTrades + " trades.");
                        System.out.println("-----------------------------------------");

                        if (arrivalTimeDeltas.isEmpty()) {
                            System.out.println("No inter-trade arrival times were calculated (fewer than 2 trades found).");
                            return;
                        }

                        conversionStartTime = System.nanoTime();
                        long[] deltasArray = arrivalTimeDeltas.stream().mapToLong(Long::longValue).toArray();
                        conversionEndTime = System.nanoTime();

                        sortStartTime = System.nanoTime();
                        Arrays.parallelSort(deltasArray);
                        sortEndTime = System.nanoTime();

                        totalEndTime = System.nanoTime();

                        printCdfSummary(deltasArray);
                        printPerformanceSummary(totalStartTime, setupEndTime, readingStartTime, readingEndTime, conversionStartTime, conversionEndTime, sortStartTime, sortEndTime, totalEndTime, totalBatchLoadNanos, totalMemCopyNanos, totalVectorCalcNanos);

                    }
                }
            }
        } catch (IOException e) {
            System.err.println("An error occurred while reading the Arrow file.");
            e.printStackTrace();
        } catch (Exception e) {
            handleProcessingException(e);
        }
    }

    private static void printCdfSummary(long[] sortedDeltas) {
        System.out.println("Analysis Results: Inter-Trade Arrival Time CDF");
        System.out.printf("Total calculated time differences: %,d%n", sortedDeltas.length);

        double[] percentiles = {0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999, 1.00};

        System.out.println("Percentiles (time in milliseconds):");
        for (double percentile : percentiles) {
            int index = (int) Math.min(Math.floor(percentile * sortedDeltas.length), sortedDeltas.length - 1);
            long value = sortedDeltas[index];
            System.out.printf("  - %.1fth percentile (%.2f): <= %d ms%n", percentile * 100, percentile, value);
        }
        System.out.println("-----------------------------------------");
    }

    private static void printPerformanceSummary(long totalStartTime, long setupEndTime, long readingStartTime, long readingEndTime, long conversionStartTime, long conversionEndTime, long sortStartTime, long sortEndTime, long totalEndTime, long totalBatchLoadNanos, long totalMemCopyNanos, long totalVectorCalcNanos) {
        double setupDuration = (setupEndTime - totalStartTime) / 1_000_000.0;
        double readingDuration = (readingEndTime - readingStartTime) / 1_000_000.0;
        double conversionDuration = (conversionEndTime - conversionStartTime) / 1_000_000.0;
        double sortDuration = (sortEndTime - sortStartTime) / 1_000_000.0;
        double totalDuration = (totalEndTime - totalStartTime) / 1_000_000.0;
        double batchLoadMillis = totalBatchLoadNanos / 1_000_000.0;
        double memCopyMillis = totalMemCopyNanos / 1_000_000.0;
        double vectorCalcMillis = totalVectorCalcNanos / 1_000_000.0;


        System.out.println("Performance Breakdown:");
        System.out.printf("  - Setup & Initialization:      %.3f ms%n", setupDuration);
        System.out.printf("  - Reading & Delta Calculation: %.3f ms%n", readingDuration);
        System.out.printf("    - I/O (Loading Batches):     %.3f ms%n", batchLoadMillis);
        System.out.printf("    - Arrow to long[] Copy:      %.3f ms%n", memCopyMillis);
        System.out.printf("    - Vector API Delta Calc:     %.3f ms%n", vectorCalcMillis);
        System.out.printf("  - List to Array Conversion:    %.3f ms%n", conversionDuration);
        System.out.printf("  - Parallel Sort:               %.3f ms%n", sortDuration);
        System.out.println("-----------------------------------------");
        System.out.printf("Total time to read and compute CDF: %.3f milliseconds%n", totalDuration);
        System.out.println("-----------------------------------------");
    }

    private static void handleProcessingException(Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("arrow-compression")) {
            System.err.println("\nERROR: Missing Compression Library. Add 'org.apache.arrow:arrow-compression' to your pom.xml.");
        } else if (e instanceof NullPointerException) {
            System.err.println("\nERROR: A column was not found. Most likely, the 'timestamp_millis_utc' column is missing from the Arrow file's schema.");
        } else {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        String arrowFile = "data/cboe/normalized/EURUSD.cboe.ny.trades.arrows";
        int iterations = 5;
        System.out.println("Running " + iterations + " iterations to measure performance...");
        for (int i = 0; i < iterations; i++) {
            System.out.println("\n--- Iteration " + (i + 1) + " ---");
            analyzeTrades(arrowFile);
        }
    }
}