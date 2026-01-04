package clickhouse;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ParquetToArrow {

    /**
     * Converts a Parquet file to Arrow format using the clickhouse-local binary.
     *
     * @param clickhouseBinaryPath Path to the 'clickhouse-local' executable (e.g., "/usr/bin/clickhouse-local" or just "clickhouse-local" if in PATH)
     * @param inputParquetPath     Path to the source Parquet file.
     * @param outputArrowPath      Path where the Arrow file should be saved.
     */
    public static void convert(String clickhouseBinaryPath, String inputParquetPath, String outputArrowPath) {

        // 1. Validate Input File Exists
        File inputFile = new File(inputParquetPath);
        if (!inputFile.exists()) {
            System.err.println("Error: Input file does not exist -> " + inputParquetPath);
            return;
        }

        // 2. Construct the SQL Query
        // Syntax: SELECT * FROM file('<path>', Parquet)
        String query = String.format("SELECT * FROM file('%s', Parquet)", inputParquetPath);

        // 3. Build the Command
        // Equivalent terminal command:
        // clickhouse-local --query "SELECT..." --format Arrow
        ProcessBuilder pb = new ProcessBuilder(
                clickhouseBinaryPath,
                "--query", query,
                "--format", "Arrow"
        );

        // 4. Configure Output Redirection
        // This takes the standard output of the command (the Arrow bytes) and saves them to the file.
        File outputFile = new File(outputArrowPath);
        pb.redirectOutput(outputFile);

        // Redirect errors to the Java console so you can see if ClickHouse complains
        pb.redirectError(ProcessBuilder.Redirect.INHERIT);

        System.out.println("Starting conversion...");
        System.out.println("Input:  " + inputParquetPath);
        System.out.println("Output: " + outputArrowPath);

        try {
            // 5. Execute the Process
            Process process = pb.start();

            // Wait for the process to finish
            int exitCode = process.waitFor();

            if (exitCode == 0) {
                System.out.println("✅ Success! File saved to: " + outputArrowPath);
                System.out.println("File size: " + Files.size(Paths.get(outputArrowPath)) + " bytes");
            } else {
                System.err.println("❌ Conversion failed. ClickHouse exited with code: " + exitCode);
            }

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    // --- Main Method for Testing ---
    public static void main(String[] args) {
        // CONFIGURATION:

        // A: Where is your clickhouse binary?
        // If it is installed globally, just use "clickhouse-local"
        // If it is in the current folder, use "./clickhouse-local" or the full path.
        String binary = "clickhouse-local";

        // B: Your file paths
        String sourceFile = "data/cboe/normalized/EURUSD.cboe.ny.trades.parquet";
        String destFile = "data/cboe/normalized/EURUSD.cboe.ny.trades.clickhouse.parquet";

        // Create a dummy parquet file for testing if it doesn't exist?
        // (You normally provide your own, but this ensures the code doesn't crash immediately)
        if (!new File(sourceFile).exists()) {
            System.out.println("⚠️ Note: '" + sourceFile + "' not found. Please provide a real path.");
        }

        // Run the converter
        convert(binary, sourceFile, destFile);
    }
}