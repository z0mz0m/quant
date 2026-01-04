package arrow;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorSpecies;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.List;

/**
 * A high-performance utility to analyze trade data from Arrow files using:
 * 1. Off-Heap Memory Access (Project Panama/Foreign Memory)
 * 2. SIMD Vector Instructions (Vector API)
 * 3. Zero-Copy Primitive Buffering
 *
 * NOTE: This class uses incubator features. Compile and run with:
 * --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector
 */
public class OffHeapReader {

    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
    private static final ByteOrder ARROW_ORDER = ByteOrder.LITTLE_ENDIAN;

    // Initial size for the results array (e.g., 10 million trades).
    // It will auto-grow, but starting large avoids early resizing.
    private static final int INITIAL_CAPACITY = 10_000_000;

    public static void analyzeTrades(String arrowFilePath) {
        long totalStartTime = System.nanoTime();

        File arrowFile = new File(arrowFilePath);
        if (!arrowFile.exists()) {
            System.err.println("Error: File not found at " + arrowFilePath);
            return;
        }

        // Timing variables
        long setupEndTime;
        long readingStartTime;
        long readingEndTime;
        long sortStartTime;
        long sortEndTime;
        long totalEndTime;

        // Granular metrics
        long totalBatchLoadNanos = 0;
        long totalVectorCalcNanos = 0;
        readingStartTime = System.nanoTime();
        try (RootAllocator allocator = new RootAllocator()) {
            try (RandomAccessFile raf = new RandomAccessFile(arrowFile, "r")) {
                try (FileChannel fileChannel = raf.getChannel()) {
                    try (ArrowFileReader reader = new ArrowFileReader(fileChannel, allocator)) {

                        System.out.println("Opened Arrow file: " + arrowFilePath);
                        VectorSchemaRoot root = reader.getVectorSchemaRoot();



                        // --- ZERO-COPY BUFFERING STRATEGY ---
                        // Instead of List<Long> (boxing), we use a raw long[] that grows.
                        long[] allDeltas = new long[INITIAL_CAPACITY];
                        int totalDeltaCount = 0;

                        long lastTimestamp = -1;
                        long totalTrades = 0;

                        setupEndTime = System.nanoTime();

                        List<ArrowBlock> recordBatches = reader.getRecordBlocks();
                        System.out.println("Processing " + recordBatches.size() + " batches...");

                        long tLoadStart = System.nanoTime();

                        for (ArrowBlock block : recordBatches) {

                            if (!reader.loadRecordBatch(block)) {
                                throw new IOException("Could not load record batch " + block.getOffset());
                            }
                            totalBatchLoadNanos += (System.nanoTime() - tLoadStart);

                            int batchSize = root.getRowCount();
                            if (batchSize == 0) continue;

                            // Grow result array if needed
                            if (totalDeltaCount + batchSize > allDeltas.length) {
                                // Double the capacity (standard ArrayList growth strategy)
                                int newCapacity = (int) Math.min((long)allDeltas.length * 2L, Integer.MAX_VALUE - 8);
                                if (newCapacity < totalDeltaCount + batchSize) newCapacity = totalDeltaCount + batchSize + 1024;
                                allDeltas = Arrays.copyOf(allDeltas, newCapacity);
                            }

                            BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp_millis_utc");


                            // 1. Determine length (in bytes)
                            int dataLength = batchSize * 8;

                            // 2. Create MemorySegment
                            // We use offset 0 because getDataBuffer() returns a buffer that
                            // already points to the start of this specific vector's data.
                            MemorySegment segment = MemorySegment.ofBuffer(timestampVector.getDataBuffer().nioBuffer(0, dataLength));



                            long tCalcStart = System.nanoTime();

                            // --- PROCESS BATCH ---

                            // Step A: Handle boundary (first element of batch uses last element of previous batch)
                            long firstVal = segment.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, 0); // Read index 0
                            if (lastTimestamp != -1) {
                                allDeltas[totalDeltaCount++] = firstVal - lastTimestamp;
                            }

                            // Step B: SIMD Loop (Vector API)
                            // We start at index 1 because we need (i) and (i-1)
                            int i = 1;
                            int loopBound = SPECIES.loopBound(batchSize);

                            // The Vector Loop: reads directly from off-heap memory
                            for (; i < loopBound; i += SPECIES.length()) {
                                long offsetCurr = (long) i * 8L;
                                long offsetPrev = (long) (i - 1) * 8L;

                                // Load 'N' longs from index i
                                LongVector vCurr = LongVector.fromMemorySegment(SPECIES, segment, offsetCurr, ARROW_ORDER);
                                // Load 'N' longs from index i-1 (Unaligned load)
                                LongVector vPrev = LongVector.fromMemorySegment(SPECIES, segment, offsetPrev, ARROW_ORDER);

                                // Compute deltas in parallel
                                LongVector vDelta = vCurr.sub(vPrev);

                                // Write directly to primitive array
                                vDelta.intoArray(allDeltas, totalDeltaCount);
                                totalDeltaCount += SPECIES.length();
                            }

                            // Step C: Tail Loop (Process remaining items scalar)
                            for (; i < batchSize; i++) {
                                long currentVal = segment.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, (long)i * 8L);
                                long prevVal    = segment.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, (long)(i-1) * 8L);
                                allDeltas[totalDeltaCount++] = currentVal - prevVal;
                            }

                            // Update state for next batch
                            lastTimestamp = segment.get(java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED, (long)(batchSize - 1) * 8L);

                            totalVectorCalcNanos += (System.nanoTime() - tCalcStart);
                            totalTrades += batchSize;
                        }
                        readingEndTime = System.nanoTime();

                        System.out.println("Finished reading " + totalTrades + " trades.");

                        if (totalDeltaCount == 0) {
                            System.out.println("No inter-trade arrival times calculated.");
                            return;
                        }

                        sortStartTime = System.nanoTime();
                        // Trim array to exact size before sorting
                        // (This replaces the expensive Stream/Map/Conversion phase)
                        long[] finalDeltas = (allDeltas.length == totalDeltaCount)
                                ? allDeltas
                                : Arrays.copyOf(allDeltas, totalDeltaCount);


                        Arrays.parallelSort(finalDeltas);
                        sortEndTime = System.nanoTime();

                        totalEndTime = System.nanoTime();

                        printCdfSummary(finalDeltas);
                        printPerformanceSummary(totalStartTime, setupEndTime, readingStartTime, readingEndTime, sortStartTime, sortEndTime, totalEndTime, totalBatchLoadNanos, totalVectorCalcNanos);

                    }
                }
            }
        } catch (Exception e) {
            handleProcessingException(e);
        }
    }

    private static void printCdfSummary(long[] sortedDeltas) {
        System.out.println("-----------------------------------------");
        System.out.println("Analysis Results: Inter-Trade Arrival Time CDF");
        System.out.printf("Total calculated time differences: %,d%n", sortedDeltas.length);

        double[] percentiles = {0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 0.999, 1.00};

        System.out.println("Percentiles (ms):");
        for (double percentile : percentiles) {
            int index = (int) Math.min(Math.floor(percentile * sortedDeltas.length), sortedDeltas.length - 1);
            long value = sortedDeltas[index];
            System.out.printf("  - %5.1f%%: <= %d ms%n", percentile * 100, value);
        }
        System.out.println("-----------------------------------------");
    }

    private static void printPerformanceSummary(long totalStartTime, long setupEndTime, long readingStartTime, long readingEndTime, long sortStartTime, long sortEndTime, long totalEndTime, long totalBatchLoadNanos, long totalVectorCalcNanos) {
        double setupDuration = (setupEndTime - totalStartTime) / 1_000_000.0;
        double readingDuration = (readingEndTime - readingStartTime) / 1_000_000.0;
        double sortDuration = (sortEndTime - sortStartTime) / 1_000_000.0;
        double totalDuration = (totalEndTime - totalStartTime) / 1_000_000.0;
        System.out.println("this should be zero:" +(totalEndTime - sortEndTime) / 1_000_000.0);
        double batchLoadMillis = totalBatchLoadNanos / 1_000_000.0;
        double vectorCalcMillis = totalVectorCalcNanos / 1_000_000.0;
        // The overhead is what's left after loading and calc (e.g. array resizing, object overhead)
        double overheadMillis = readingDuration - (batchLoadMillis + vectorCalcMillis);

        System.out.println("Performance Breakdown:");
        System.out.printf("  - Setup & Initialization:       %8.3f ms%n", setupDuration);
        System.out.printf("  - Reading & Calculation (Total):%8.3f ms%n", readingDuration);
        System.out.printf("    |-- Disk I/O (Load Batch):    %8.3f ms%n", batchLoadMillis);
        System.out.printf("    |-- Zero-Copy Vector Calc:    %8.3f ms%n", vectorCalcMillis);
        System.out.printf("    |-- Logic Overhead:           %8.3f ms%n", overheadMillis);
        System.out.printf("  - Parallel Sort:                %8.3f ms%n", sortDuration);
        System.out.println("-----------------------------------------");
        System.out.printf("Total Execution Time:             %8.3f ms%n", totalDuration);
        System.out.println("-----------------------------------------");
    }

    private static void handleProcessingException(Exception e) {
        if (e.getMessage() != null && e.getMessage().contains("arrow-compression")) {
            System.err.println("\nERROR: Missing Compression Library. Add 'org.apache.arrow:arrow-compression' to your pom.xml.");
        } else if (e instanceof NullPointerException) {
            e.printStackTrace();
            System.err.println("\nERROR: Possible missing column or null pointer. Check schema.");
        } else {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        // Update path as needed
        String arrowFile = "data/cboe/normalized/EURUSD.cboe.ny.trades.clickhouse.nocompression.arrow";

        // Warmup / Performance Test
        int iterations = 5;
        System.out.println("Running " + iterations + " iterations to measure performance...");
        for (int i = 0; i < iterations; i++) {
            System.out.println("\n--- Iteration " + (i + 1) + " ---");
            analyzeTrades(arrowFile);

            // Suggest GC between runs for cleaner numbers
            System.gc();
        }
    }
}