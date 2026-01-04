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
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;



public class ArrowOffHeapParallelLoad {




    private static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_PREFERRED;
    private static final ByteOrder ARROW_ORDER = ByteOrder.LITTLE_ENDIAN;

    public static void analyzeTrades(String arrowFilePath, long[] reusableBuffer) {
        long totalStartTime = System.nanoTime();
        File arrowFile = new File(arrowFilePath);

        // Timers
        long ioSetupStart, ioSetupEnd, parallelReadStart, parallelReadEnd;
        long sortStart, sortEnd;

        // Use a SHARED Arena so multiple threads can write to the allocated segment
        try (Arena arena = Arena.ofShared()) {

            // 1. Setup Phase: Read File Footer to get Block Metadata
            ioSetupStart = System.nanoTime();
            List<ArrowBlock> blocks;
            try (FileChannel fc = FileChannel.open(arrowFile.toPath(), StandardOpenOption.READ);
                 RootAllocator setupAllocator = new RootAllocator();
                 ArrowFileReader metadataReader = new ArrowFileReader(fc, setupAllocator)) {

                // This is fast (just reads the footer)
                blocks = metadataReader.getRecordBlocks();
            } catch (IOException e) {
                e.printStackTrace();
                return;
            }
            ioSetupEnd = System.nanoTime();

            // Allocate Off-Heap Memory
            MemorySegment globalSegment = arena.allocate((long) reusableBuffer.length * 8L, 8);

            // Atomic counter to manage where threads write their results
            AtomicLong globalWriteIndex = new AtomicLong(0);

            // 2. Parallel Processing Phase
            parallelReadStart = System.nanoTime();

            blocks.parallelStream().forEach(block -> {
                // Each thread needs its own resources to be thread-safe
                try (RootAllocator threadAllocator = new RootAllocator();
                     FileChannel threadChannel = FileChannel.open(arrowFile.toPath(), StandardOpenOption.READ);
                     ArrowFileReader threadReader = new ArrowFileReader(threadChannel, threadAllocator)) {

                    // Load the specific block assigned to this thread
                    if (!threadReader.loadRecordBatch(block)) return;

                    VectorSchemaRoot root = threadReader.getVectorSchemaRoot();
                    int batchSize = root.getRowCount();
                    if (batchSize < 2) return; // Need at least 2 rows for a delta

                    BigIntVector timestampVector = (BigIntVector) root.getVector("timestamp_millis_utc");

                    // Access Raw Data
                    MemorySegment inputSegment = MemorySegment.ofBuffer(
                            timestampVector.getDataBuffer().nioBuffer(0,  batchSize * 8)
                    );

                    // Reserve space in the global buffer for this batch's results
                    // Note: We calculate (batchSize - 1) deltas per batch
                    long myStartIndex = globalWriteIndex.getAndAdd((batchSize - 1) * 8L);

                    // Perform Vector Calculation
                    processBatch(inputSegment, batchSize, globalSegment, myStartIndex);

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            parallelReadEnd = System.nanoTime();

            long totalBytesWritten = globalWriteIndex.get();
            if (totalBytesWritten == 0) {
                System.out.println("No trades found.");
                return;
            }

            // 3. Sorting Phase (On-Heap)
            // Copy from Off-Heap to Reusable Heap Array
            MemorySegment heapSegment = MemorySegment.ofArray(reusableBuffer);
            MemorySegment.copy(globalSegment, 0, heapSegment, 0, totalBytesWritten);

            int validCount = (int) (totalBytesWritten / 8);

            sortStart = System.nanoTime();
            Arrays.parallelSort(reusableBuffer, 0, validCount);
            sortEnd = System.nanoTime();

            printPerformanceSummary(totalStartTime, ioSetupStart, ioSetupEnd, parallelReadStart, parallelReadEnd, sortStart, sortEnd);
            printCdfSummary(reusableBuffer, validCount);
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

    // Extracted Vector Logic to keep the stream clean
    private static void processBatch(MemorySegment input, int batchSize, MemorySegment output, long writeOffset) {
        int i = 1;
        int loopBound = SPECIES.loopBound(batchSize - 1);

        // Vector Loop using Unaligned Loads
        for (; i < loopBound; i += SPECIES.length()) {
            long offsetCurr = (long) i * 8L;
            long offsetPrev = (long) (i - 1) * 8L;

            LongVector curr = LongVector.fromMemorySegment(SPECIES, input, offsetCurr, ARROW_ORDER);
            LongVector prev = LongVector.fromMemorySegment(SPECIES, input, offsetPrev, ARROW_ORDER);

            LongVector diff = curr.sub(prev);

            diff.intoMemorySegment(output, writeOffset, ByteOrder.LITTLE_ENDIAN);
            writeOffset += SPECIES.length() * 8L;
        }

        // Tail Loop
        for (; i < batchSize; i++) {
            long curr = input.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), (long) i * 8L);
            long prev = input.get(ValueLayout.JAVA_LONG.withOrder(ARROW_ORDER), (long) (i - 1) * 8L);
            output.set(ValueLayout.JAVA_LONG_UNALIGNED, writeOffset, curr - prev);
            writeOffset += 8L;
        }
    }

    // ... [Reuse your existing printCdfSummary and main methods] ...

    private static void printPerformanceSummary(long start, long ioStart, long ioEnd, long readStart, long readEnd, long sortStart, long sortEnd) {
        System.out.println("Performance Breakdown:");
        System.out.printf("  - Metadata Scan:       %.3f ms%n", (ioEnd - ioStart) / 1e6);
        System.out.printf("  - Parallel Read/Calc:  %.3f ms%n", (readEnd - readStart) / 1e6);
        System.out.printf("  - Sorting:             %.3f ms%n", (sortEnd - sortStart) / 1e6);
        System.out.printf("  - Total Time:          %.3f ms%n", (System.nanoTime() - start) / 1e6);
        System.out.println("-----------------------------------------");
    }

    // Quick Main for copy-paste
    public static void main(String[] args) {
        String arrowsFile = "data/cboe/normalized/EDF_OUTPUT_NY_20251205.threesixtyt.lob.clickhouse.nocompression.arrow";
        long[] reusableBuffer = new long[100_000_000];

        for (int i = 0; i < 5; i++) {
            System.out.println("\n--- Iteration " + (i + 1) + " ---");
            analyzeTrades(arrowsFile, reusableBuffer);
        }
    }
}