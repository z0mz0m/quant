package cboe;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * A utility class to read and parse trade data from a gzipped CSV file using DuckDB.
 */
public class DuckDbGzTradeReader {

    /**
     * Reads trades from a gzipped CSV file, processes them, and saves them as a ZSTD-compressed Parquet file.
     *
     * @param gzFilePath        Path to the input .csv.gz file.
     * @param outputParquetPath Path for the output .parquet file.
     */
    public static void convertAndSaveAsParquet(String gzFilePath, String outputParquetPath) {
        String jdbcUrl = "jdbc:duckdb:"; // Use an in-memory DuckDB database

        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("DuckDB JDBC driver not found.");
            e.printStackTrace();
            return;
        }

        // This SQL query instructs DuckDB to:
        // 1. Read the gzipped CSV and generate timestamps with sub-millisecond indices.
        // 2. Use the COPY statement to save the result directly to a Parquet file.
        // 3. The output Parquet file will use ZSTD compression.
        String sql = String.format(
                "COPY (" +
                        "  WITH TradesWithUTCTimestamp AS ( " +
                        "    SELECT " +
                        "      raw_ts, " +
                        "      epoch_ms(CAST(raw_ts AS TIMESTAMP) AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS utc_timestamp_ms, " +
                        "      (CAST(raw_ts AS TIMESTAMP) AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS utc_timestamp, " +
                        "      side, " +
                        "      price, " +
                        "      size " +
                        "    FROM read_csv_auto('%s', " +
                        "      header=false, " +
                        "      columns={'raw_ts': 'VARCHAR', 'side': 'VARCHAR', 'price': 'DOUBLE', 'size': 'BIGINT'}" +
                        "    )" +
                        "  ) " +
                        "  SELECT " +
                        "    utc_timestamp, " +
                        "    utc_timestamp_ms, " +
                        "    (ROW_NUMBER() OVER (PARTITION BY utc_timestamp_ms ORDER BY raw_ts) - 1) as sub_ms_idx, " +
                        "    side as tx_agg_side, " +
                        "    price*1e9 as scaled_tx_px, " +
                        "    size as tx_sz" +
                        "  FROM TradesWithUTCTimestamp " +
                        "  ORDER BY utc_timestamp_ms, sub_ms_idx" +
                        ") TO '%s' (FORMAT PARQUET, COMPRESSION 'ZSTD')",
                gzFilePath.replace("\\", "/"),
                outputParquetPath.replace("\\", "/")
        );

        System.out.println("Executing conversion with DuckDB:");
        System.out.printf("  -> Input: %s%n", gzFilePath);
        System.out.printf("  -> Output: %s%n", outputParquetPath);


        try (
                Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()
        ) {
            stmt.execute(sql);
            System.out.println("Conversion successful.");

        } catch (Exception e) {
            System.err.println("An error occurred during DuckDB execution.");
            e.printStackTrace();
        }
    }

    /**
     * Main method to demonstrate the reader function.
     */
    public static void main(String[] args) {
        String inputFile = "data/cboe/trades/ny/trd_ny_2025-11-24_EURUSD.csv.gz";
        String outputFile = "data/cboe/normalized/trd_ny_2025-11-24_EURUSD.cboe.trades.parquet";

        // Ensure the output directory exists
        File output = new File(outputFile);
        if (output.getParentFile() != null) {
            output.getParentFile().mkdirs();
        }

        convertAndSaveAsParquet(inputFile, outputFile);
    }
}