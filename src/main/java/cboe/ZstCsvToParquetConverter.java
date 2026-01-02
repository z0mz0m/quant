package cboe;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ZstCsvToParquetConverter {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ZstCsvToParquetConverter <input-directory> <output-directory>");
            System.exit(1);
        }

        File inputDir = new File(args[0]);
        File outputDir = new File(args[1]);

        if (!inputDir.isDirectory()) {
            System.err.println("Error: Input path is not a valid directory.");
            return;
        }

        if (!outputDir.exists() && !outputDir.mkdirs()) {
            System.err.println("Error: Could not create output directory.");
            return;
        }

        File[] zstFiles = inputDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".zst"));
        if (zstFiles == null || zstFiles.length == 0) {
            System.out.println("No .zst files found in the specified directory.");
            return;
        }

        String jdbcUrl = "jdbc:duckdb:"; // In-memory DuckDB
        Class.forName("org.duckdb.DuckDBDriver");

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
            Statement stmt = conn.createStatement()) {
            stmt.execute("SET TimeZone = 'UTC'");

            for (File inputFile : zstFiles) {
                String inputFileName = inputFile.getName();
                String outputFileName;

                // Determine the output file name by replacing the extension
                if (inputFileName.toLowerCase().endsWith(".csv.zst")) {
                    outputFileName = inputFileName.substring(0, inputFileName.length() - ".csv.zst".length()) + ".parquet";
                } else {
                    outputFileName = inputFileName.substring(0, inputFileName.length() - ".zst".length()) + ".parquet";
                }

                File outputFile = new File(outputDir, outputFileName);

                String inputFilePathSql = inputFile.getAbsolutePath().replace("\\", "/");
                String outputFilePathSql = outputFile.getAbsolutePath().replace("\\", "/");

                System.out.printf("Converting '%s' to '%s'%n", inputFile.getPath(), outputFile.getPath());

                try {
                    // Simplified query: read_csv_auto infers the schema, and we sort by the numeric timestamp.
                    String exportSql = String.format(
                            "COPY (" +
                                    "  SELECT * " +
                                    "  FROM read_csv_auto('%s') " +
                                    "  ORDER BY timestamp_millis_utc" +
                                    ") TO '%s' (FORMAT PARQUET, CODEC 'ZSTD', COMPRESSION_LEVEL 9)",
                            inputFilePathSql, outputFilePathSql
                    );

                    stmt.execute(exportSql);
                    System.out.println("  -> Conversion complete: " + outputFile.getPath());

                } catch (SQLException e) {
                    System.err.println("  -> Failed to convert file: " + inputFile.getPath());
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(2);
        }
    }
}