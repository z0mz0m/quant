package cboe;

import java.sql.*;

/**
 * A utility class to read and parse trade data from a gzipped CSV file using DuckDB.
 */
public class DuckDbGzTradeReader {

    /**
     * Reads trades from a gzipped CSV file using DuckDB and prints them to the console.
     * The file is expected to have no header and columns for timestamp, side, price, and size.
     *
     * @param gzFilePath Path to the input .csv.gz file.
     */
    public static void readAndDisplayTrades(String gzFilePath) {
        String jdbcUrl = "jdbc:duckdb:"; // Use an in-memory DuckDB database

        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("DuckDB JDBC driver not found.");
            e.printStackTrace();
            return;
        }

        // This SQL query instructs DuckDB to:
        // 1. Define a Common Table Expression (CTE) to first parse the timestamps.
        // 2. Use the ROW_NUMBER() window function to generate a sub-millisecond index.
        //    - It partitions the data by millisecond (`utc_timestamp_ms`).
        //    - It orders by the raw timestamp string to preserve the original file order.
        //    - Subtracts 1 to make the index 0-based.
        String sql = String.format(
                "WITH TradesWithUTCTimestamp AS ( " +
                        "  SELECT " +
                        "    raw_ts, " +
                        "    epoch_ms(CAST(raw_ts AS TIMESTAMP) AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS utc_timestamp_ms, " +
                        "    (CAST(raw_ts AS TIMESTAMP) AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS utc_timestamp, " +
                        "    side, " +
                        "    price, " +
                        "    size " +
                        "  FROM read_csv_auto('%s', " +
                        "    header=false, " +
                        "    columns={'raw_ts': 'VARCHAR', 'side': 'VARCHAR', 'price': 'DOUBLE', 'size': 'BIGINT'}" +
                        "  )" +
                        ") " +
                        "SELECT " +
                        "  raw_ts, " +
                        "  utc_timestamp, " +
                        "  utc_timestamp_ms, " +
                        "  (ROW_NUMBER() OVER (PARTITION BY utc_timestamp_ms ORDER BY raw_ts) - 1) as sub_ms_idx, " +
                        "  side, " +
                        "  price, " +
                        "  size " +
                        "FROM TradesWithUTCTimestamp " +
                        "ORDER BY utc_timestamp_ms, sub_ms_idx " + // Order by the new index
                        "LIMIT 20",
                gzFilePath.replace("\\", "/") // Ensure forward slashes for the path
        );

        System.out.println("Executing query with DuckDB:");
        System.out.println(sql);

        try (
                Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(sql)
        ) {
            System.out.println("\n--- DuckDB Query Results ---");
            while (rs.next()) {

                String rawTimestamp = rs.getString("raw_ts");
                long utcTimestamp_ms = rs.getLong("utc_timestamp_ms");
                Timestamp utcTimestamp = rs.getTimestamp("utc_timestamp");
                int subMsIdx = rs.getInt("sub_ms_idx");
                String side = rs.getString("side");
                double price = rs.getDouble("price");
                long size = rs.getLong("size");
                System.out.printf(
                        "  -> Raw: %s, UTC: %s, UTC ms: %d, SubMS: %d, Side: %s, Price: %.5f, Size: %d%n",
                        rawTimestamp, utcTimestamp, utcTimestamp_ms, subMsIdx, side, price, size
                );


            }
            System.out.println("--- End of Results ---\n");

        } catch (Exception e) {
            System.err.println("An error occurred during DuckDB execution.");
            e.printStackTrace();
        }
    }

    /**
     * Main method to demonstrate the reader function.
     */
    public static void main(String[] args) {
        // IMPORTANT: Update this path to a valid .csv.gz file in your project.
        String filePath = "data/cboe/trades/ny/trd_ny_2025-11-24_EURUSD.csv.gz";
        readAndDisplayTrades(filePath);
    }
}