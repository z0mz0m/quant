package cboe;



import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.zip.GZIPInputStream;

/**
 * A utility class to read and parse trade data from a gzipped CSV file.
 */
public class GzTradeReader {

    // Formatter for the input timestamp, e.g., "2025-11-24 18:00:02.939"
    private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Reads a gzipped CSV file containing trades, parses each line, and prints the result to the console.
     *
     * @param gzFilePath The path to the input .csv.gz file.
     * @throws IOException if an I/O error occurs.
     */
    public static void readTradesFromGz(String gzFilePath) throws IOException {
        System.out.println("Reading trades from: " + gzFilePath);

        try (
                InputStream fileStream = Files.newInputStream(Paths.get(gzFilePath));
                GZIPInputStream gzipStream = new GZIPInputStream(fileStream);
                InputStreamReader reader = new InputStreamReader(gzipStream);
                BufferedReader bufferedReader = new BufferedReader(reader)
        ) {
            String line;
            int lineNum = 0;
            while ((line = bufferedReader.readLine()) != null) {
                lineNum++;
                String[] columns = line.trim().split(",");
                if (columns.length < 4) {
                    System.err.printf("Skipping malformed line %d: %s%n", lineNum, line);
                    continue;
                }

                try {
                    // 1. Parse Timestamp
                    String timestampStr = columns[0];
                    LocalDateTime timestamp = LocalDateTime.parse(timestampStr, INPUT_FORMATTER);

                    // 2. Parse Side
                    String side = columns[1];

                    // 3. Parse Price
                    double price = Double.parseDouble(columns[2]);

                    // 4. Parse Size
                    long size = Long.parseLong(columns[3]);

                    // Print the parsed data to verify correctness
                    System.out.printf(
                            "  - Parsed Line %d: Timestamp=%s, Side=%s, Price=%.5f, Size=%d%n",
                            lineNum, timestamp, side, price, size
                    );

                } catch (DateTimeParseException e) {
                    System.err.printf("Could not parse timestamp on line %d: '%s'%n", lineNum, columns[0]);
                } catch (NumberFormatException e) {
                    System.err.printf("Could not parse price/size on line %d: '%s'%n", lineNum, line);
                }
            }
        }
        System.out.println("Finished reading file.");
    }

    /**
     * Main method to demonstrate the reader function.
     * You will need to change the file path to a valid file in your project.
     */
    public static void main(String[] args) {
        // Example usage:
        // Replace this with a valid path to one of your .csv.gz files.
        String filePath = "data/cboe/trades/ny/trd_ny_2025-11-03_EURUSD.csv.gz";
        try {
            readTradesFromGz(filePath);
        } catch (IOException e) {
            System.err.println("An error occurred while reading the file: " + e.getMessage());
            e.printStackTrace();
        }
    }
}