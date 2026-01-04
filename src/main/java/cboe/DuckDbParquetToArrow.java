package cboe;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;



public class DuckDbParquetToArrow {

    /**
     * Loads data from a Parquet file and saves it as an Arrow file, excluding one column.
     *
     * @param inputParquetPath Path to the input .parquet file.
     * @param outputArrowPath  Path for the output .arrow file.
     */
    public static void convertParquetToArrow(String inputParquetPath, String outputArrowPath) {
        String jdbcUrl = "jdbc:duckdb:"; // Use an in-memory DuckDB database

        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            System.err.println("DuckDB JDBC driver not found.");
            e.printStackTrace();
            return;
        }

        // This SQL query instructs DuckDB to:
        // 1. Read the Parquet file.
        // 2. Select all columns *except* 'utc_timestamp'.
        // 3. Save the result to a new file in the Arrow format.
        String sql = String.format(
                "COPY (" +
                        "  SELECT timestamp_millis_utc, sym, source " +
                        "  FROM read_parquet('%s')" +
                        ") TO '%s' (FORMAT ARROW)",
                inputParquetPath.replace("\\", "/"),
                outputArrowPath.replace("\\", "/")
                //COPY arrow_libraries TO 'test.arrows' (FORMAT ARROWS, BATCH_SIZE 100);
        );

        System.out.println("Executing Parquet to Arrow conversion:");
        System.out.printf("  -> Input: %s%n", inputParquetPath);
        System.out.printf("  -> Output: %s%n", outputArrowPath);

        try (
                Connection conn = DriverManager.getConnection(jdbcUrl);
                Statement stmt = conn.createStatement()
        ) {
            // Install and load the Arrow extension
            stmt.execute("INSTALL arrow FROM community");
            stmt.execute("LOAD arrow");

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
        String parquetFile = "data/cboe/normalized/EDF_OUTPUT_NY_20251205.threesixtyt.lob.parquet";
        String arrowFile = "data/cboe/normalized/EDF_OUTPUT_NY_20251205.threesixtyt.lob.arrows";

        // Ensure the output directory exists
        File output = new File(arrowFile);
        if (output.getParentFile() != null) {
            output.getParentFile().mkdirs();
        }

        convertParquetToArrow(parquetFile, arrowFile);
    }



}
