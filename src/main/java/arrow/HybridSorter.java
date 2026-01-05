package arrow;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.IntStream;

public class HybridSorter {

    // Threshold: 2 million ms (approx 33 minutes).
    // 99.99% of high-frequency trade deltas fall below this.
    private static final int THRESHOLD = 2_000_000;

    /**
     * Sorts the array in O(N) time by exploiting the distribution of trade deltas.
     */
    public static void sort(long[] data, int length) {
        // 1. Determine concurrency level (use all cores)
        int numThreads = Runtime.getRuntime().availableProcessors();
        int chunkSize = (length + numThreads - 1) / numThreads;

        // Shared container for outliers (thread-safe)
        // We expect very few outliers, so CopyOnWrite or sync list is fine.
        CopyOnWriteArrayList<Long> outliers = new CopyOnWriteArrayList<>();

        // 2. Parallel "Map" Phase: Build local histograms
        // We use parallel streams to process chunks of the main array
        int[][] partialHistograms = IntStream.range(0, numThreads).parallel().mapToObj(threadIdx -> {
            int start = threadIdx * chunkSize;
            int end = Math.min(start + chunkSize, length);

            // 8MB local buffer (fits in L3 cache on server CPUs)
            int[] localCounts = new int[THRESHOLD];

            for (int i = start; i < end; i++) {
                long val = data[i];

                // FIX: Check val >= 0 to prevent negative index crash
                if (val >= 0 && val < THRESHOLD) {
                    localCounts[(int) val]++;
                } else {
                    // Negative values (bad data) and large values go here
                    outliers.add(val);
                }
            }
            return localCounts;
        }).toArray(int[][]::new);

        // 3. Sequential "Reduce" Phase: Merge histograms
        // Merging int arrays is extremely fast (SIMD optimized by JIT)
        int[] globalCounts = new int[THRESHOLD];
        for (int[] local : partialHistograms) {
            for (int i = 0; i < THRESHOLD; i++) {
                globalCounts[i] += local[i];
            }
        }

        // 4. Reconstruct the sorted array
        int writeIndex = 0;

        // A. Fill the small values (Linear reconstruction)
        for (int val = 0; val < THRESHOLD; val++) {
            int count = globalCounts[val];
            if (count > 0) {
                // Optimization: Arrays.fill is faster than a loop for runs
                Arrays.fill(data, writeIndex, writeIndex + count, val);
                writeIndex += count;
            }
        }

        // B. Sort and append outliers
        if (!outliers.isEmpty()) {
            int outlierCount = outliers.size();
            long[] outlierArr = new long[outlierCount];
            for (int i = 0; i < outlierCount; i++) {
                outlierArr[i] = outliers.get(i);
            }
            Arrays.sort(outlierArr); // Standard sort for the few remaining items
            System.arraycopy(outlierArr, 0, data, writeIndex, outlierCount);
        }
    }
}