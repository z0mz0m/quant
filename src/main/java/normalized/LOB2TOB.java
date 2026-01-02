package normalized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

public class LOB2TOB {
    public static void main(String[] args) throws Exception {
        String inDirPath = "data/lob-normalized/dec5";
        String outDirPath = "data/tob-normalized/dec5";

        File outDir = new File(outDirPath);
        if (!outDir.exists() && !outDir.mkdirs()) {
            System.err.println("Failed to create output directory: " + outDirPath);
            return;
        }

        String jdbcUrl = "jdbc:duckdb:"; // In-process DuckDB
        Class.forName("org.duckdb.DuckDBDriver");

        try (Connection conn = DriverManager.getConnection(jdbcUrl);
             Statement stmt = conn.createStatement()) {

            stmt.execute("SET TimeZone = 'UTC'");

            try (Stream<Path> paths = Files.walk(Paths.get(inDirPath))) {
                paths
                        .filter(Files::isRegularFile)
                        .filter(path -> path.toString().endsWith(".lob.csv.zst"))
                        .forEach(inputPath -> {
                            try {
                                String inputFileName = inputPath.getFileName().toString();
                                String outputFileName = inputFileName.replace(".lob.csv.zst", ".tob.csv.zst");
                                Path outputPath = Paths.get(outDirPath, outputFileName);

                                String inputFilePathSql = inputPath.toAbsolutePath().toString().replace("\\", "/").replace("'", "''");
                                String outputFilePathSql = outputPath.toAbsolutePath().toString().replace("\\", "/").replace("'", "''");

                                System.out.printf("Processing %s -> %s%n", inputPath, outputPath);

                                String tobSql = String.format(
                                        "SELECT strftime(timestamp, '%%Y-%%m-%%dT%%H:%%M:%%S.%%gZ') AS timestamp, " +
                                                "timestamp_millis_utc, bid_px_00, bid_sz_00, ask_px_00, ask_sz_00, sym, source " +
                                                "FROM read_csv_auto('%s') WHERE page_index=0 ORDER BY timestamp",
                                        inputFilePathSql
                                );

                                String exportSql = String.format(
                                        "COPY (%s) TO '%s' (FORMAT CSV, HEADER, COMPRESSION ZSTD)",
                                        tobSql, outputFilePathSql
                                );

                                stmt.execute(exportSql);
                                System.out.println("  -> Export complete: " + outputPath);

                            } catch (SQLException e) {
                                throw new RuntimeException("Error processing file: " + inputPath, e);
                            }
                        });
            }
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            System.exit(2);
        }
    }
}