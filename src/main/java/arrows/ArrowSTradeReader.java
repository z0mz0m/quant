package arrows;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * A utility class to read and analyze trade data from an Arrow file.
 */
public class ArrowSTradeReader {

    private static final byte[] ARROW_MAGIC = "ARROW1".getBytes(StandardCharsets.US_ASCII);

    /**
     * Analyzes an Arrow file to calculate the CDF of inter-trade arrival times.
     *
     * @param arrowFilePath The path to the Arrow file.
     */
    public static void analyzeTrades(String arrowFilePath) {
        long totalStartTime = System.nanoTime(); // Start the overall timer

        File arrowFile = new File(arrowFilePath);
        if (!arrowFile.exists()) {
            System.err.println("Error: File not found at " + arrowFilePath);
            return;
        }

        if (!isArrowFileValid(arrowFile)) {
            System.err.println("Warning: File may not be a valid Arrow IPC file (magic number mismatch).");
        }

        long setupEndTime;
        long readingStartTime;
        long readingEndTime;
        long sortStartTime;
        long sortEndTime;
        long totalEndTime;

        try (RootAllocator allocator = new RootAllocator();
             FileInputStream fileInputStream = new FileInputStream(arrowFile);
             ArrowStreamReader reader = new ArrowStreamReader(fileInputStream, allocator)) {

            System.out.println("Successfully opened Arrow file: " + arrowFilePath);
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            System.out.println("Schema: " + root.getSchema());
            System.out.println("-----------------------------------------");
            System.out.println("Calculating inter-trade arrival times...");

            setupEndTime = System.nanoTime(); // End setup time after reader is initialized

            List<Long> arrivalTimeDeltas = new ArrayList<>();
            long lastTimestamp = -1;
            long totalTrades = 0;

            readingStartTime = System.nanoTime();
            while (reader.loadNextBatch()) {
                int batchSize = root.getRowCount();
                BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp_millis_utc");

                for (int i = 0; i < batchSize; i++) {
                    long currentTimestamp = timestampVector.get(i);
                    if (lastTimestamp != -1) {
                        long delta = currentTimestamp - lastTimestamp;
                        arrivalTimeDeltas.add(delta);
                    }
                    lastTimestamp = currentTimestamp;
                }
                totalTrades += batchSize;
            }
            readingEndTime = System.nanoTime();

            System.out.println("Finished reading " + totalTrades + " trades.");
            System.out.println("-----------------------------------------");

            if (arrivalTimeDeltas.isEmpty()) {
                System.out.println("No inter-trade arrival times were calculated (fewer than 2 trades found).");
                return;
            }

            sortStartTime = System.nanoTime();
            Collections.sort(arrivalTimeDeltas);
            sortEndTime = System.nanoTime();

            totalEndTime = System.nanoTime();

            printCdfSummary(arrivalTimeDeltas);
            printPerformanceSummary(totalStartTime, setupEndTime, readingStartTime, readingEndTime, sortStartTime, sortEndTime, totalEndTime);

        } catch (IOException e) {
            System.err.println("An error occurred while reading the Arrow file.");
            e.printStackTrace();
        } catch (Exception e) {
            handleProcessingException(e);
        }
    }

    private static void printPerformanceSummary(long totalStartTime, long setupEndTime, long readingStartTime, long readingEndTime, long sortStartTime, long sortEndTime, long totalEndTime) {
        double setupDuration = (setupEndTime - totalStartTime) / 1_000_000.0;
        double readingDuration = (readingEndTime - readingStartTime) / 1_000_000.0;
        double sortDuration = (sortEndTime - sortStartTime) / 1_000_000.0;
        double totalDuration = (totalEndTime - totalStartTime) / 1_000_000.0;

        System.out.println("Performance Breakdown:");
        System.out.printf("  - Setup & Initialization:      %.3f ms%n", setupDuration);
        System.out.printf("  - Reading & Delta Calculation: %.3f ms%n", readingDuration);
        System.out.printf("  - List Sort:                   %.3f ms%n", sortDuration);
        System.out.println("-----------------------------------------");
        System.out.printf("Total time to read and compute CDF: %.3f milliseconds%n", totalDuration);
        System.out.println("-----------------------------------------");
    }

    /**
     * Prints a summary of the Cumulative Distribution Function (CDF) using percentiles.
     */
    private static void printCdfSummary(List<Long> sortedDeltas) {
        System.out.println("Analysis Results: Inter-Trade Arrival Time CDF");
        System.out.printf("Total calculated time differences: %,d%n", sortedDeltas.size());

        // Define the percentiles we want to calculate
        double[] percentiles = {0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999, 1.00};

        System.out.println("Percentiles (time in milliseconds):");
        for (double percentile : percentiles) {
            int index = (int) Math.min(Math.floor(percentile * sortedDeltas.size()), sortedDeltas.size() - 1);
            long value = sortedDeltas.get(index);
            System.out.printf("  - %.1fth percentile (%.2f): <= %d ms%n", percentile * 100, percentile, value);
        }
        System.out.println("-----------------------------------------");
    }

    /**
     * Provides more helpful error messages for common issues.
     */
    private static void handleProcessingException(Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("arrow-compression")) {
            System.err.println("\nERROR: Missing Compression Library. Add 'org.apache.arrow:arrow-compression' to your pom.xml.");
        } else if (e instanceof NullPointerException) {
            System.err.println("\nERROR: A column was not found. Most likely, the 'timestamp_millis_utc' column is missing from the Arrow file's schema.");
        } else {
            e.printStackTrace();
        }
    }

    private static boolean isArrowFileValid(File file) {
        if (file.length() < ARROW_MAGIC.length * 2) return false;
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            byte[] header = new byte[ARROW_MAGIC.length];
            byte[] footer = new byte[ARROW_MAGIC.length];
            raf.readFully(header);
            raf.seek(file.length() - ARROW_MAGIC.length);
            raf.readFully(footer);
            return Arrays.equals(header, ARROW_MAGIC) && Arrays.equals(footer, ARROW_MAGIC);
        } catch (IOException e) {
            return false;
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