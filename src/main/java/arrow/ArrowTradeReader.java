package arrow;

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
public class ArrowTradeReader {

    private static final byte[] ARROW_MAGIC = "ARROW1".getBytes(StandardCharsets.US_ASCII);

    /**
     * Analyzes an Arrow file to calculate the CDF of inter-trade arrival times.
     *
     * @param arrowFilePath The path to the Arrow file.
     */
    public static void analyzeTrades(String arrowFilePath) {
        File arrowFile = new File(arrowFilePath);
        if (!arrowFile.exists()) {
            System.err.println("Error: File not found at " + arrowFilePath);
            return;
        }

        if (!isArrowFileValid(arrowFile)) {
            System.err.println("Warning: File may not be a valid Arrow IPC file (magic number mismatch).");
        }

        try (RootAllocator allocator = new RootAllocator();
             FileInputStream fileInputStream = new FileInputStream(arrowFile);
             ArrowStreamReader reader = new ArrowStreamReader(fileInputStream, allocator)) {

            System.out.println("Successfully opened Arrow file: " + arrowFilePath);
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            System.out.println("Schema: " + root.getSchema());
            System.out.println("-----------------------------------------");
            System.out.println("Calculating inter-trade arrival times...");

            List<Long> arrivalTimeDeltas = new ArrayList<>();
            long lastTimestamp = -1;
            long totalTrades = 0;

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

            System.out.println("Finished reading " + totalTrades + " trades.");
            System.out.println("-----------------------------------------");

            if (arrivalTimeDeltas.isEmpty()) {
                System.out.println("No inter-trade arrival times were calculated (fewer than 2 trades found).");
                return;
            }

            // Sort the deltas to calculate the CDF
            Collections.sort(arrivalTimeDeltas);
            printCdfSummary(arrivalTimeDeltas);

        } catch (IOException e) {
            System.err.println("An error occurred while reading the Arrow file.");
            e.printStackTrace();
        } catch (Exception e) {
            handleProcessingException(e);
        }
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
        String arrowFile = "data/cboe/normalized/trd_ny_2025-11-24_EURUSD.cboe.trades4.arrows";
        analyzeTrades(arrowFile);
    }
}