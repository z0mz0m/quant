package normalized;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TradesJoiner {
    public static void main(String[] args) throws Exception {
        String inDirPath = "data/trades-normalized";
        String outDirPath = "data/trades-joined";
        String outputFileName = "all.trades.csv.zst";

        File outDir = new File(outDirPath);
        if (!outDir.exists() && !outDir.mkdirs()) {
            System.err.println("Failed to create output directory: " + outDirPath);
            return;
        }

        String inputGlobPath = new File(inDirPath).getAbsolutePath().replace("\\", "/") + "/*.trades.csv.zst";
        String outputFilePath = new File(outDirPath, outputFileName).getAbsolutePath().replace("\\", "/");

        String jdbcUrl = "jdbc:duckdb:"; // In-memory DuckDB
        Class.forName("org.duckdb.DuckDBDriver");

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()) {

            // Set the timezone to UTC for the session
            stmt.execute("SET TimeZone = 'UTC'");

            System.out.printf("Joining files from '%s' into '%s'%n", inputGlobPath, outputFilePath);

            // SQL to read all matching files, union them, format the timestamp, and sort chronologically
            String joinSql = String.format(
                    "SELECT strftime(timestamp, '%%Y-%%m-%%dT%%H:%%M:%%S.%%gZ') AS timestamp, " +
                            "timestamp_millis_utc, tx_px, tx_sz, ts_agg_side, sym, source " +
                            "FROM read_csv_auto('%s') ORDER BY timestamp",
                    inputGlobPath
            );

            // SQL to export the result of the query to a new compressed file
            String exportSql = String.format(
                    "COPY (%s) TO '%s' (FORMAT CSV, HEADER, COMPRESSION ZSTD)",
                    joinSql, outputFilePath
            );

            stmt.execute(exportSql);
            System.out.println("  -> Join and export complete: " + outputFilePath);

        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(2);
        }
    }
}