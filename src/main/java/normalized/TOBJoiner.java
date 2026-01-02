package normalized;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Collectors;

public class TOBJoiner {
    public static void main(String[] args) throws Exception {
        String inDirPath = "data/tob-normalized/dec5";
        String outDirPath = "data/tob-joined/dec5";
        String outputFileName;

        String whereClause = "";
        // If command-line arguments are provided, use them as symbol filters.
        if (args.length > 0) {
            System.out.println("Filtering for symbols: " + Arrays.toString(args));
            // Sanitize each symbol and format for the SQL IN clause.
            String symbolsForInClause = Arrays.stream(args)
                    .map(s -> "'" + s.replace("'", "''") + "'")
                    .collect(Collectors.joining(", "));
            whereClause = "WHERE sym IN (" + symbolsForInClause + ")";

            // Adjust output file name based on the number of symbols
            if (args.length == 1) {
                outputFileName = args[0] + ".tob.csv.zst";
            } else {
                outputFileName = "custom_selection.tob.csv.zst";
            }
        } else {
            System.out.println("No symbol filter provided. Joining all symbols.");
            outputFileName = "all2.tob.csv.zst";
        }

        File outDir = new File(outDirPath);
        if (!outDir.exists() && !outDir.mkdirs()) {
            System.err.println("Failed to create output directory: " + outDirPath);
            return;
        }

        String inputGlobPath = new File(inDirPath).getAbsolutePath().replace("\\", "/") + "/*.tob.csv.zst";
        String outputFilePath = new File(outDirPath, outputFileName).getAbsolutePath().replace("\\", "/");

        String jdbcUrl = "jdbc:duckdb:";
        Class.forName("org.duckdb.DuckDBDriver");

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()) {

            stmt.execute("SET TimeZone = 'UTC'");
            System.out.printf("Joining files from '%s' into '%s'%n", inputGlobPath, outputFilePath);

            // The SQL query now includes the dynamically generated WHERE clause
            String joinSql = String.format(
                    "SELECT strftime(CAST(timestamp AS TIMESTAMP), '%%Y-%%m-%%dT%%H:%%M:%%S.%%gZ') AS timestamp, " +
                            "timestamp_millis_utc, bid_px_00, bid_sz_00, ask_px_00, ask_sz_00, sym, source " +
                            "FROM read_csv_auto('%s') %s ORDER BY timestamp",
                    inputGlobPath, whereClause
            );

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