package arrow;



import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * A utility class to read trade data from an Arrow file using memory-mapping.
 */
public class ArrowTradeReader {

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

        // The RootAllocator is the parent for all memory allocations.
        // Use ArrowStreamReader for the Arrow streaming format.
        try (RootAllocator allocator = new RootAllocator();
             FileInputStream fileInputStream = new FileInputStream(arrowFile);
             ArrowStreamReader reader = new ArrowStreamReader(fileInputStream, allocator)) {

            System.out.println("Successfully opened Arrow file: " + arrowFilePath);

            // VectorSchemaRoot provides a view over a set of Arrow vectors (columns).
            // For a stream, the schema is read first, so we can access it after initialization.
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            System.out.println("Schema: " + root.getSchema());

            long totalTrades = 0;

            // The reader loads the file in chunks (record batches). We iterate through them.
            while (reader.loadNextBatch()) {
                // getRowCount() gives the number of rows (trades) in the current batch.
                totalTrades += root.getRowCount();
            }

            System.out.println("-----------------------------------------");
            System.out.println("Analysis Results:");
            System.out.println("Total number of trades: " + totalTrades);
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

    public static void main(String[] args) {
        String arrowFile = "data/cboe/normalized/trd_ny_2025-11-24_EURUSD.cboe.trades4.arrow";
        //String arrowFile = "data/cboe/normalized/output.arrow";
        analyzeTrades(arrowFile);
    }
}