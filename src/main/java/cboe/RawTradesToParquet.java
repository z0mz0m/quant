package cboe;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class RawTradesToParquet {

    public static void main(String[] args) {
        if (args.length != 4) {
            System.err.println("Usage: RawTradesToParquet <input-directory> <output-file> <sym> <source>");
            System.exit(1);
        }

        String inDirPath = args[0];
        String outputFile = args[1];
        String sym = args[2];
        String source = args[3];

        Path outputFilePath = Paths.get(outputFile);
        File outputDir = outputFilePath.getParent().toFile();
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            System.err.println("Failed to create output directory: " + outputDir);
            return;
        }

        String inputGlobPath = new File(inDirPath, "*.csv.gz").getPath().replace('\\', '/');
        String finalOutputFile = outputFilePath.toAbsolutePath().toString().replace("\\", "/");

        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("DuckDB JDBC driver not found.");
            e.printStackTrace();
            return;
        }

        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
             Statement stmt = conn.createStatement()) {

            stmt.execute("SET TimeZone = 'UTC'");

            System.out.printf("Converting all files matching '%s' to a single Parquet file '%s'%n", inputGlobPath, finalOutputFile);

            String conversionSql = String.format(
                    "COPY (" +
                            "  SELECT " +
                            "    SUBSTRING(strftime(utc_ts, '%%Y-%%m-%%dT%%H:%%M:%%S.%%fZ'), 1, 23) || 'Z' AS timestamp, " +
                            "    epoch_ms(utc_ts) AS timestamp_millis_utc, " +
                            "    (price * 1e9)::BIGINT AS tx_px, " +
                            "    volume::BIGINT AS tx_sz, " +
                            "    side AS ts_agg_side, " +
                            "    '%s' AS sym, " +
                            "    '%s' AS source " +
                            "  FROM (" +
                            "    SELECT " +
                            "      strptime(trade_time, '%%Y-%%m-%%d %%H:%%M:%%S.%%f')::TIMESTAMP AT TIME ZONE 'America/New_York' AT TIME ZONE 'UTC' AS utc_ts, " +
                            "      price, " +
                            "      volume, " +
                            "      side " +
                            "    FROM read_csv_auto('%s', header=false, columns={'trade_time': 'VARCHAR', 'side': 'VARCHAR', 'price': 'DOUBLE', 'volume': 'BIGINT'})" +
                            "  )" +
                            "  ORDER BY timestamp_millis_utc" +
                            ") TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD);",
                    sym, source, inputGlobPath, finalOutputFile
            );

            stmt.execute(conversionSql);
            System.out.println("  -> Conversion and union complete: " + finalOutputFile);

        } catch (SQLException e) {
            System.err.println("An error occurred during SQL execution: " + e.getMessage());
            e.printStackTrace();
        }
    }
}