package arrow;



import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * A utility class to read trade data from an Arrow file using memory-mapping.
 */
public class ArrowTradeReader {

    private static final byte[] ARROW_MAGIC = "ARROW1".getBytes(StandardCharsets.US_ASCII);

    /**
     * Analyzes an Arrow file to perform operations like counting total trades.
     *
     * @param arrowFilePath The path to the Arrow file.
     */
    public static void analyzeTrades(String arrowFilePath) {
        File arrowFile = new File(arrowFilePath);
        if (!arrowFile.exists()) {
            System.err.println("Error: File not found at " + arrowFilePath);
            return;
        }

        // First, validate the Arrow magic number at the start and end of the file.
        if (!isArrowFileValid(arrowFile)) {
            System.err.println("-----------------------------------------");
            System.err.println("ERROR: Invalid or Corrupt Arrow File");
            System.err.println("-----------------------------------------");
            System.err.println("The file is missing the 'ARROW1' magic number at the beginning or end.");
            System.err.println("This indicates that it is either not a valid Arrow stream file or it is truncated/corrupt.");
            System.err.println("-----------------------------------------");
            //return;
        }

        // The RootAllocator is the parent for all memory allocations.
        // Use ArrowStreamReader for the Arrow streaming format.
        try (RootAllocator allocator = new RootAllocator();
             FileInputStream fileInputStream = new FileInputStream(arrowFile);
             ArrowStreamReader reader = new ArrowStreamReader(fileInputStream, allocator)) {

            System.out.println("Successfully opened and validated Arrow file: " + arrowFilePath);

            // VectorSchemaRoot provides a view over a set of Arrow vectors (columns).
            // For a stream, the schema is read first, so we can access it after initialization.
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            System.out.println("Schema: " + root.getSchema());
            System.out.println("-----------------------------------------");
            System.out.println("Reading record batches...");

            long totalTrades = 0;
            int batchCount = 0;

            // The reader loads the file in chunks (record batches). We iterate through them.
            while (reader.loadNextBatch()) {
                // getRowCount() gives the number of rows (trades) in the current batch. This is the batch size.
                int batchSize = root.getRowCount();
                totalTrades += batchSize;
                batchCount++;
                System.out.printf("  -> Read Batch #%d: size = %d trades%n", batchCount, batchSize);
            }

            System.out.println("-----------------------------------------");
            System.out.println("Analysis Results:");
            System.out.printf("Total number of batches: %d%n", batchCount);
            System.out.printf("Total number of trades: %d%n", totalTrades);
            System.out.println("-----------------------------------------");

        } catch (IOException e) {
            System.err.println("An error occurred while reading the Arrow file.");
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            // Provide a more informative error message for the specific compression issue.
            if (e.getMessage() != null && e.getMessage().contains("arrow-compression")) {
                System.err.println("\n-----------------------------------------");
                System.err.println("ERROR: Missing Compression Library Module");
                System.err.println("-----------------------------------------");
                System.err.println("The Arrow file is compressed, but the required 'arrow-compression' module was not found.");
                System.err.println("\nSOLUTION: To read this file, please add the 'org.apache.arrow:arrow-compression' dependency to your project's build configuration (e.g., pom.xml or build.gradle) and rebuild.");
                System.err.println("-----------------------------------------");
            } else {
                // Re-throw other unexpected IllegalArgumentExceptions
                throw e;
            }
        }
    }

    /**
     * Checks for the 'ARROW1' magic number at the beginning and end of a file.
     *
     * @param file The file to validate.
     * @return True if the file has the correct magic numbers, false otherwise.
     */
    private static boolean isArrowFileValid(File file) {
        // File must be at least 12 bytes long to hold two magic numbers.
        if (file.length() < ARROW_MAGIC.length * 2) {
            return false;
        }

        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            byte[] header = new byte[ARROW_MAGIC.length];
            byte[] footer = new byte[ARROW_MAGIC.length];

            // Read the magic number from the beginning of the file.
            raf.readFully(header);

            // Seek to the end of the file to read the footer magic number.
            raf.seek(file.length() - ARROW_MAGIC.length);
            raf.readFully(footer);

            // Both header and footer must match the 'ARROW1' magic number.
            return Arrays.equals(header, ARROW_MAGIC) && Arrays.equals(footer, ARROW_MAGIC);

        } catch (IOException e) {
            System.err.println("Could not read file for validation: " + e.getMessage());
            return false;
        }
    }


    public static void main(String[] args) {
        String arrowFile = "data/cboe/normalized/trd_ny_2025-11-24_EURUSD.cboe.trades4.arrows";
        //String arrowFile = "data/cboe/normalized/output.arrow";
        analyzeTrades(arrowFile);
    }
}