package arrow;



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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class ArrowOffHeap {

    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
    private static final ByteOrder ARROW_ORDER = ByteOrder.LITTLE_ENDIAN;

    /**
     * Analyzes an Arrow stream file using a reusable buffer to minimize GC.
     */
    public static void analyzeTrades(String arrowFilePath, long[] reusableBuffer) {
        long totalStartTime = System.nanoTime();

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
        long totalBatchLoadNanos = 0;
        long totalSegmentSetupNanos = 0;
        long totalVectorCalcNanos = 0;

        try (Arena arena = Arena.ofConfined()) {
            // Ensure our off-heap buffer matches the size of our reusable heap buffer
            long maxTradesBytes = (long) reusableBuffer.length * 8L;
            MemorySegment arrivalTimeDeltasMS = arena.allocate(maxTradesBytes, 8);

            long writeOffset = 0; // Tracks bytes written

            try (RootAllocator allocator = new RootAllocator()) {
                try (FileChannel fileChannel = FileChannel.open(arrowFile.toPath(), StandardOpenOption.READ);
                     ArrowFileReader reader = new ArrowFileReader(fileChannel, allocator))
                {

                    System.out.println("Successfully opened Arrow stream file: " + arrowFilePath);
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    setupEndTime = System.nanoTime();

                    long lastTimestamp = -1;
                    long totalTrades = 0;
                    long t1, t2;

                    readingStartTime = System.nanoTime();
                    long batchReadStart = System.nanoTime();

                    while (reader.loadNextBatch()) {
                        totalBatchLoadNanos += (System.nanoTime() - batchReadStart);

                        int batchSize = root.getRowCount();
                        if (batchSize == 0) continue;

                        BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp_millis_utc");

                        t1 = System.nanoTime();
                        // Create segment from buffer
                        java.nio.ByteBuffer byteBuffer = timestampVector.getDataBuffer().nioBuffer(0, batchSize * Long.BYTES);
                        MemorySegment segment = MemorySegment.ofBuffer(byteBuffer);
                        t2 = System.nanoTime();
                        totalSegmentSetupNanos += (t2 - t1);

                        t1 = System.nanoTime();

                        // 1. Boundary Logic
                        if (lastTimestamp != -1) {
                            long firstTimestamp = segment.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), 0);
                            long boundaryDelta = firstTimestamp - lastTimestamp;
                            arrivalTimeDeltasMS.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, boundaryDelta);
                            writeOffset += 8L;
                        }

                        int i = 1;

                        // 2. Vector Loop (Using Shuffle as requested)
                        for (; i <= batchSize - SPECIES.length(); i += SPECIES.length()) {
                            LongVector current = LongVector.fromMemorySegment(SPECIES, segment, (long) i * Long.BYTES, ARROW_ORDER);
                            VectorShuffle<Long> slide = VectorShuffle.iota(SPECIES, 1, 1, true);
                            LongVector prev = current.rearrange(slide);
                            LongVector delta = current.sub(prev);

                            delta.intoMemorySegment(arrivalTimeDeltasMS, writeOffset, ByteOrder.LITTLE_ENDIAN);
                            writeOffset += SPECIES.length() * 8L;
                        }

                        // 3. Tail Loop
                        for (; i < batchSize; i++) {
                            long current = segment.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), (long) i * Long.BYTES);
                            long previous = segment.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), (long) (i - 1) * Long.BYTES);
                            arrivalTimeDeltasMS.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, current - previous);
                            writeOffset += 8L;
                        }
                        t2 = System.nanoTime();
                        totalVectorCalcNanos += (t2 - t1);

                        lastTimestamp = segment.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), (long) (batchSize - 1) * Long.BYTES);
                        totalTrades += batchSize;
                        batchReadStart = System.nanoTime();
                    }
                    readingEndTime = System.nanoTime();

                    System.out.println("Finished reading " + totalTrades + " trades.");

                    if (writeOffset == 0) {
                        System.out.println("No inter-trade arrival times were calculated.");
                        return;
                    }

                    // --- IMPROVEMENT START: Reusing Heap Array ---

                    conversionStartTime = System.nanoTime();

                    // 1. Wrap the reusable long[] in a MemorySegment
                    MemorySegment heapSegment = MemorySegment.ofArray(reusableBuffer);

                    // 2. Bulk copy from Off-Heap (arrivalTimeDeltasMS) to On-Heap (reusableBuffer)
                    //    We only copy exactly the number of bytes we wrote.
                    MemorySegment.copy(arrivalTimeDeltasMS, 0, heapSegment, 0, writeOffset);

                    int validCount = (int) (writeOffset / 8);

                    conversionEndTime = System.nanoTime();

                    sortStartTime = System.nanoTime();
                    // 3. Sort ONLY the valid portion of the array (from 0 to validCount)
                    Arrays.parallelSort(reusableBuffer, 0, validCount);
                    sortEndTime = System.nanoTime();

                    // --- IMPROVEMENT END ---

                    totalEndTime = System.nanoTime();

                    printCdfSummary(reusableBuffer, validCount);
                    printPerformanceSummary(totalStartTime, setupEndTime, readingStartTime, readingEndTime, conversionStartTime, conversionEndTime, sortStartTime, sortEndTime, totalEndTime, totalBatchLoadNanos, totalSegmentSetupNanos, totalVectorCalcNanos);

                }
            } catch (IOException e) {
                System.err.println("An error occurred while reading the Arrow file.");
                e.printStackTrace();
            } catch (Exception e) {
                handleProcessingException(e);
            }
        }
    }

    // Updated to accept 'count' so we ignore the trailing zeros in the large buffer
    private static void printCdfSummary(long[] sortedDeltas, int count) {
        System.out.println("Analysis Results: Inter-Trade Arrival Time CDF");
        System.out.printf("Total calculated time differences: %,d%n", count);

        double[] percentiles = {0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999, 1.00};

        System.out.println("Percentiles (time in milliseconds):");
        for (double percentile : percentiles) {
            // Calculate index based on 'count', not array.length
            int index = (int) Math.min(Math.floor(percentile * count), count - 1);
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
        System.out.printf("  - Off-Heap to Heap Copy:       %.3f ms%n", conversionDuration); // Renamed label
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
        String arrowsFile = "data/cboe/normalized/EDF_OUTPUT_NY_20251205.threesixtyt.lob.clickhouse.nocompression.arrow";
        int iterations = 10;

        // --- IMPROVEMENT: Allocate once, outside the loop ---
        // 100 Million trades buffer (approx 800MB heap usage)
        System.out.println("Allocating reusable heap buffer...");
        long[] reusableBuffer = new long[100_000_000];

        System.out.println("Running " + iterations + " iterations to measure performance...");
        for (int i = 0; i < iterations; i++) {
            System.out.println("\n--- Iteration " + (i + 1) + " ---");
            // Pass the buffer into the method
            analyzeTrades(arrowsFile, reusableBuffer);
        }
    }
}