package arrow;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A utility class to read and analyze trade data from an Arrow file using memory mapping for efficient I/O,
 * the Java Vector API for calculations, and parallel sorting for improved performance.
 * NOTE: This class uses an incubator feature. The code must be compiled and run with:
 * --add-modules jdk.incubator.vector
 */
public class ArrowTradeVectorReader {

    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;

    /**
     * Analyzes an Arrow file to calculate the CDF of inter-trade arrival times.
     *
     * @param arrowFilePath The path to the Arrow file.
     */
    public static void analyzeTrades(String arrowFilePath) {
        long startTime = System.nanoTime(); // Start the timer

        File arrowFile = new File(arrowFilePath);
        if (!arrowFile.exists()) {
            System.err.println("Error: File not found at " + arrowFilePath);
            return;
        }

        // Use RandomAccessFile to get a FileChannel for memory mapping
        try (RootAllocator allocator = new RootAllocator();
             RandomAccessFile raf = new RandomAccessFile(arrowFile, "r");
             FileChannel fileChannel = raf.getChannel();
             ArrowFileReader reader = new ArrowFileReader(new SeekableReadChannel(fileChannel), allocator)) {

            System.out.println("Successfully opened Arrow file with memory mapping: " + arrowFilePath);
            System.out.println("-----------------------------------------");
            System.out.println("Calculating inter-trade arrival times using Vector API...");

            List<Long> arrivalTimeDeltas = new ArrayList<>();
            long lastTimestamp = -1;
            long totalTrades = 0;

            long[] deltaChunk = new long[SPECIES.length()];

            // Iterate through the record batches in the file
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                if (!reader.loadRecordBatch(arrowBlock)) {
                    continue; // Skip if loading fails
                }
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                int batchSize = root.getRowCount();
                if (batchSize == 0) continue;

                System.out.printf("Loaded batch of %d trades.%n", batchSize);
                BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp_millis_utc");

                long[] batchTimestamps = new long[batchSize];
                for (int i = 0; i < batchSize; i++) {
                    batchTimestamps[i] = timestampVector.get(i);
                }

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

                lastTimestamp = batchTimestamps[batchSize - 1];
                totalTrades += batchSize;
            }

            System.out.println("Finished reading " + totalTrades + " trades.");
            System.out.println("-----------------------------------------");

            if (arrivalTimeDeltas.isEmpty()) {
                System.out.println("No inter-trade arrival times were calculated (fewer than 2 trades found).");
                return;
            }

            long[] deltasArray = arrivalTimeDeltas.stream().mapToLong(Long::longValue).toArray();
            Arrays.parallelSort(deltasArray);

            long endTime = System.nanoTime(); // End the timer after computation
            double durationMillis = (endTime - startTime) / 1_000_000.0;

            printCdfSummary(deltasArray);

            System.out.printf("Total time to read and compute CDF: %.3f milliseconds%n", durationMillis);
            System.out.println("-----------------------------------------");

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
        analyzeTrades(arrowFile);
    }
}