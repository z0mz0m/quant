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
        // 1. Read the gzipped CSV file directly from disk.
        // 2. Read the timestamp column as a plain string ('VARCHAR').
        // 3. Use the CAST function to reliably convert the string into a proper TIMESTAMP.
        //    DuckDB's CAST is smart and correctly interprets '.325' as 325 milliseconds.
        String sql = String.format(
                "SELECT " +
                        "raw_ts, " +
                        "  CAST(raw_ts AS TIMESTAMP_MS) AS trade_timestamp, " +
                        "  CAST(raw_ts AS TIME) AS trade_timestamp2, " +
                        "  (CAST(raw_ts AS TIMESTAMP) AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC') AS utc_timestamp, " +
                        "  side, " +
                        "  price, " +
                        "  size " +
                        "FROM read_csv_auto('%s', " +
                        "  header=false, " +
                        "  columns={'raw_ts': 'VARCHAR', 'side': 'VARCHAR', 'price': 'DOUBLE', 'size': 'BIGINT'}" +
                        ") " +
                        "LIMIT 20", // Limit results for a clean demonstration
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
                Timestamp timestamp = rs.getTimestamp("trade_timestamp");

                Time timestamp2 = rs.getTime("trade_timestamp2");
                Timestamp utcTimestamp = rs.getTimestamp("utc_timestamp");



                String side = rs.getString("side");
                double price = rs.getDouble("price");
                long size = rs.getLong("size");
                System.out.printf(
                        "  -> Raw: %s, T1(CAST): %s, T2(): %s,UTC timestamp:%s Side: %s, Price: %.5f, Size: %d%n",
                        rawTimestamp, timestamp, timestamp2,  utcTimestamp,side, price, size
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