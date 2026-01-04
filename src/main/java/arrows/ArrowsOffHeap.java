package arrows;



import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorShuffle;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A utility class to read and analyze trade data from an Arrow stream file (.arrows) using:
 * 1. Off-Heap Memory Access (Project Panama/Foreign Memory)
 * 2. SIMD Vector Instructions (Vector API)
 * for improved performance.
 * NOTE: This class uses incubator features. The code must be compiled and run with:
 * --add-modules jdk.incubator.vector --enable-native-access=ALL-UNNAMED
 */
public class ArrowsOffHeap {

    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
    private static final ByteOrder ARROW_ORDER = ByteOrder.LITTLE_ENDIAN;


    /**
     * Analyzes an Arrow stream file to calculate the CDF of inter-trade arrival times.
     *
     * @param arrowFilePath The path to the Arrow stream file.
     */


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


        // Create a scope for off-heap allocation
        // confined() is fastest but thread-bound; use shared() if sharing across threads
        Arena arena = Arena.ofConfined();

    // Estimate size (e.g., based on file size or a safe upper bound like 100MB)
    // You cannot "resize" this easily like an ArrayList, so start big.
        long maxTrades = 100_000_000;
        MemorySegment arrivalTimeDeltasMS = arena.allocate(maxTrades * 8L, 8); // 8 bytes per long, 8-byte alignment

        // Keep track of where we are writing (in bytes)
        long writeOffset = 0;


        try (RootAllocator allocator = new RootAllocator()) {
            try (FileInputStream fileInputStream = new FileInputStream(arrowFile);
                 ArrowStreamReader reader = new ArrowStreamReader(fileInputStream, allocator)) {


                System.out.println("Successfully opened Arrow stream file: " + arrowFilePath);
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                System.out.println("Schema: " + root.getSchema());
                System.out.println("-----------------------------------------");
                System.out.println("Calculating inter-trade arrival times using Vector API and Off-Heap Memory...");

                setupEndTime = System.nanoTime();

                List<Long> arrivalTimeDeltas = new ArrayList<>();
                long lastTimestamp = -1;
                long totalTrades = 0;

                long[] deltaChunk = new long[SPECIES.length()];

                // Add accumulators for more granular timing
                long totalBatchLoadNanos = 0;
                long totalSegmentSetupNanos = 0;
                long totalVectorCalcNanos = 0;
                long t1, t2;


                readingStartTime = System.nanoTime();
                long batchReadStart = System.nanoTime();
                while (reader.loadNextBatch()) {
                    totalBatchLoadNanos += (System.nanoTime() - batchReadStart);

                    int batchSize = root.getRowCount();
                    if (batchSize == 0) continue;

                    BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp_millis_utc");

                    t1 = System.nanoTime();
                    java.nio.ByteBuffer byteBuffer = timestampVector.getDataBuffer().nioBuffer(0,  batchSize * Long.BYTES);
                    MemorySegment segment = MemorySegment.ofBuffer(byteBuffer);
                    t2 = System.nanoTime();
                    totalSegmentSetupNanos += (t2-t1);

                    t1 = System.nanoTime();
                    if (lastTimestamp != -1) {
                        long firstTimestamp = segment.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), 0);
                        arrivalTimeDeltas.add(firstTimestamp - lastTimestamp);
                    }

                    int i = 1;
                    // REMOVED: int loopBound = SPECIES.loopBound(batchSize);

                    // The loop condition is changed to be robust.
                    // It ensures that the 'current' vector can be fully loaded without reading past the end of the batch.
                    for (; i <= batchSize - SPECIES.length(); i += SPECIES.length()) {
                        LongVector current = LongVector.fromMemorySegment(SPECIES, segment, (long)i * Long.BYTES, ARROW_ORDER);
                        VectorShuffle<Long> slide = VectorShuffle.iota(SPECIES,1,1,true);
                        LongVector prev = current.rearrange(slide);
                        //LongVector previous = LongVector.fromMemorySegment(SPECIES, segment, (long)(i - 1) * Long.BYTES, ARROW_ORDER);

                        LongVector delta = current.sub(prev);

                        delta.intoMemorySegment(arrivalTimeDeltasMS, writeOffset, ByteOrder.LITTLE_ENDIAN);
                        writeOffset += SPECIES.length() * 8L;


                    }


                    for (; i < batchSize; i++) {
                        long current = segment.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), (long)i * Long.BYTES);
                        long previous = segment.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), (long)(i - 1) * Long.BYTES);
                        long delta = current - previous;
                        arrivalTimeDeltasMS.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, delta);
                        writeOffset += 8L;
                    }
                    t2 = System.nanoTime();
                    totalVectorCalcNanos += (t2 - t1);


                    lastTimestamp = segment.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), (long)(batchSize - 1) * Long.BYTES);
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
                //long[] deltasArray = arrivalTimeDeltas.stream().mapToLong(Long::longValue).toArray();

                long[] deltasArray = arrivalTimeDeltasMS.asSlice(0, writeOffset).toArray(ValueLayout.JAVA_LONG_UNALIGNED);

                conversionEndTime = System.nanoTime();

                sortStartTime = System.nanoTime();
                Arrays.parallelSort(deltasArray);
                sortEndTime = System.nanoTime();

                totalEndTime = System.nanoTime();

                printCdfSummary(deltasArray);
                printPerformanceSummary(totalStartTime, setupEndTime, readingStartTime, readingEndTime, conversionStartTime, conversionEndTime, sortStartTime, sortEndTime, totalEndTime, totalBatchLoadNanos, totalSegmentSetupNanos, totalVectorCalcNanos);

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

    private static void printPerformanceSummary(long totalStartTime, long setupEndTime, long readingStartTime, long readingEndTime, long conversionStartTime, long conversionEndTime, long sortStartTime, long sortEndTime, long totalEndTime, long totalBatchLoadNanos, long totalSegmentSetupNanos, long totalVectorCalcNanos) {
        double setupDuration = (setupEndTime - totalStartTime) / 1_000_000.0;
        double readingDuration = (readingEndTime - readingStartTime) / 1_000_000.0;
        double conversionDuration = (conversionEndTime - conversionStartTime) / 1_000_000.0;
        double sortDuration = (sortEndTime - sortStartTime) / 1_000_000.0;
        double totalDuration = (totalEndTime - totalStartTime) / 1_000_000.0;
        double batchLoadMillis = totalBatchLoadNanos / 1_000_000.0;
        double segmentSetupMillis = totalSegmentSetupNanos / 1_000_000.0;
        double vectorCalcMillis = totalVectorCalcNanos / 1_000_000.0;


        System.out.println("Performance Breakdown:");
        System.out.printf("  - Setup & Initialization:      %.3f ms%n", setupDuration);
        System.out.printf("  - Reading & Delta Calculation: %.3f ms%n", readingDuration);
        System.out.printf("    - I/O (Loading Batches):     %.3f ms%n", batchLoadMillis);
        System.out.printf("    - Off-Heap Segment Setup:    %.3f ms%n", segmentSetupMillis);
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
        String arrowsFile ;
        arrowsFile = "data/cboe/normalized/EDF_OUTPUT_NY_20251205.threesixtyt.lob.arrows";
        int iterations = 5;
        System.out.println("Running " + iterations + " iterations to measure performance...");
        for (int i = 0; i < iterations; i++) {
            System.out.println("\n--- Iteration " + (i + 1) + " ---");
            analyzeTrades(arrowsFile);
        }
    }
}


