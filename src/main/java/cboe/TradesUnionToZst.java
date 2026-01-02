package cboe;

import com.github.luben.zstd.ZstdOutputStream;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

/**
 * A utility class to read all raw trade files from a directory,
 * convert them to a normalized format, and save them as a single
 * Zstandard-compressed CSV file and a plain CSV file.
 */
public class TradesUnionToZst {

    private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final ZoneId NY_ZONE = ZoneId.of("America/New_York");
    private static final ZoneId LDN_ZONE = ZoneId.of("Europe/London");
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
    private static final Pattern SYMBOL_PATTERN = Pattern.compile("_([A-Z]+)\\.csv\\.gz$");
    private static final String SOURCE_NAME = "CBOE";

    public static void main(String[] args) throws IOException {
        String inDirPath = "data/cboe/trades/ln";
        String outZstPath = "data/trades-normalized/cboe_ny_trades_union_utc.csv.zst";
        String outCsvPath = "data/trades-normalized/cboe_ny_trades_union_utc.csv";

        Path zstPath = Paths.get(outZstPath);
        Path csvPath = Paths.get(outCsvPath);
        Files.createDirectories(zstPath.getParent());
        Files.deleteIfExists(zstPath);
        Files.deleteIfExists(csvPath);

        // Use try-with-resources for all streams and writers
        try (
                FileOutputStream fosZst = new FileOutputStream(outZstPath);
                ZstdOutputStream zos = new ZstdOutputStream(fosZst, 9); // Set compression level
                PrintWriter zstWriter = new PrintWriter(new OutputStreamWriter(zos, StandardCharsets.UTF_8));
                FileOutputStream fosCsv = new FileOutputStream(outCsvPath);
                PrintWriter csvWriter = new PrintWriter(new OutputStreamWriter(fosCsv, StandardCharsets.UTF_8))
        ) {
            // Write the header for the normalized CSV file
            String header = "timestamp,timestamp_millis_utc,tx_px,tx_sz,tx_agg_side,sym,source";
            zstWriter.println(header);
            csvWriter.println(header);

            System.out.println("Starting to process files from: " + inDirPath);

            // Walk through the input directory and process each .csv.gz file
            try (Stream<Path> paths = Files.walk(Paths.get(inDirPath))) {
                paths
                        .filter(Files::isRegularFile)
                        .filter(p -> p.toString().endsWith(".csv.gz"))
                        .forEach(p -> {
                            try {
                                processGzFile(p, zstWriter, csvWriter);
                            } catch (IOException e) {
                                System.err.println("Error processing file " + p + ": " + e.getMessage());
                                e.printStackTrace();
                            }
                        });
            }
        }

        System.out.println("Successfully created ZST-compressed CSV file at: " + outZstPath);
        System.out.println("Successfully created CSV file at: " + outCsvPath);
    }

    /**
     * Processes a single gzipped trades file and writes its content to the CSV writers.
     *
     * @param gzFilePath The path to the input .csv.gz file.
     * @param writers     The PrintWriters to write records to.
     * @throws IOException if an I/O error occurs.
     */
    private static void processGzFile(Path gzFilePath, PrintWriter... writers) throws IOException {
        System.out.println("Processing: " + gzFilePath.getFileName());

        Matcher matcher = SYMBOL_PATTERN.matcher(gzFilePath.getFileName().toString());
        String sym = matcher.find() ? matcher.group(1) : "UNKNOWN";

        try (
                InputStream fis = Files.newInputStream(gzFilePath);
                GZIPInputStream gzis = new GZIPInputStream(fis);
                InputStreamReader isr = new InputStreamReader(gzis);
                BufferedReader reader = new BufferedReader(isr)
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] cols = line.trim().split(",");
                if (cols.length < 4) continue; // Skip malformed lines

                try {
                    String nyTimestampStr = cols[0];
                    String side = cols[1];
                    String priceStr = cols[2];
                    String size = cols[3];

                    // Perform timestamp conversion
                    LocalDateTime ldt = LocalDateTime.parse(nyTimestampStr, INPUT_FORMATTER);
                    ZonedDateTime nyZonedDateTime = ldt.atZone(UTC_ZONE);
                    ZonedDateTime utczonedDateTime = nyZonedDateTime.withZoneSameInstant(UTC_ZONE);

                    String utcTimestamp = OUTPUT_FORMATTER.format(utczonedDateTime);
                    long utcTimestampMillis = utczonedDateTime.toInstant().toEpochMilli();
                    long priceNanos = (long) (Double.parseDouble(priceStr) * 1e9);

                    // Create a CSV row and write it to the output file
                    StringJoiner csvRow = new StringJoiner(",");
                    csvRow.add(utcTimestamp);
                    csvRow.add(String.valueOf(utcTimestampMillis));
                    csvRow.add(String.valueOf(priceNanos));
                    csvRow.add(size);
                    csvRow.add(side);
                    csvRow.add(sym);
                    csvRow.add(SOURCE_NAME);

                    for (PrintWriter writer : writers) {
                        writer.println(csvRow);
                    }

                } catch (DateTimeParseException | NumberFormatException e) {
                    System.err.println("Could not parse line in " + gzFilePath.getFileName() + ": " + line);
                }
            }
        }
    }
}