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
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class RAW2Trades {

    private static final DateTimeFormatter INPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final DateTimeFormatter OUTPUT_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final ZoneId NY_ZONE = ZoneId.of("America/New_York");
    private static final ZoneId UTC_ZONE = ZoneId.of("UTC");

    /**
     * Processes a gzipped trades file, normalizes it, and writes to a Zstandard compressed file.
     *
     * @param gzFilePath    Path to the input .csv.gz file.
     * @param outputZstPath Path for the output .csv.zst file.
     * @param sym           The symbol for the trades (e.g., "EURUSD").
     * @param source        The source of the data (e.g., "CBOE").
     * @throws IOException if an I/O error occurs.
     */
    public void processGzFile(String gzFilePath, String outputZstPath, String sym, String source) throws IOException {
        System.out.println("Starting processing of file: " + gzFilePath);
        System.out.println("Output will be written to: " + outputZstPath);

        try (
                InputStream fis = Files.newInputStream(Paths.get(gzFilePath));
                GZIPInputStream gzis = new GZIPInputStream(fis);
                InputStreamReader isr = new InputStreamReader(gzis);
                BufferedReader reader = new BufferedReader(isr);
                FileOutputStream fos = new FileOutputStream(outputZstPath);
                ZstdOutputStream zos = new ZstdOutputStream(fos, 9);
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(zos, StandardCharsets.UTF_8))
        ) {
            // Write header to the output file
            writer.println("timestamp,timestamp_millis_utc,tx_px,tx_sz,ts_agg_side,sym,source");

            String line;
            while ((line = reader.readLine()) != null) {
                String[] cols = line.trim().split(",");
                if (cols.length < 4) {
                    continue; // Skip malformed lines
                }

                String nyTimestampStr = cols[0];
                String side = cols[1];
                String price = cols[2];
                String size = cols[3];

                String utcTimestamp;
                String utcTimestampMillis;

                try {
                    LocalDateTime ldt = LocalDateTime.parse(nyTimestampStr, INPUT_FORMATTER);
                    ZonedDateTime nyZonedDateTime = ldt.atZone(NY_ZONE);
                    ZonedDateTime utczonedDateTime = nyZonedDateTime.withZoneSameInstant(UTC_ZONE);
                    utcTimestamp = OUTPUT_FORMATTER.format(utczonedDateTime);
                    utcTimestampMillis = String.valueOf(utczonedDateTime.toInstant().toEpochMilli());
                    long priceNanos = (long) (Double.parseDouble(price) * 1e9);

                    StringJoiner csvRow = new StringJoiner(",");
                    csvRow.add(utcTimestamp);
                    csvRow.add(utcTimestampMillis);
                    csvRow.add(String.valueOf(priceNanos));
                    csvRow.add(size);
                    csvRow.add(side);
                    csvRow.add(sym);
                    csvRow.add(source);

                    writer.println(csvRow);

                } catch (DateTimeParseException e) {
                    System.err.println("Could not parse timestamp: " + nyTimestampStr);
                    utcTimestamp = "INVALID_TIMESTAMP";
                    utcTimestampMillis = "INVALID_TIMESTAMP";
                }
            }
        }
    }

    public static void main(String[] args) {
        String inDirPath = "data/cboe/trades";
        String outDirPath = "data/trades-normalized";

        File outDir = new File(outDirPath);
        if (!outDir.exists() && !outDir.mkdirs()) {
            System.err.println("Failed to create output directory: " + outDirPath);
            return;
        }

        try (Stream<Path> paths = Files.walk(Paths.get(inDirPath))) {
            paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.toString().endsWith(".csv.gz"))
                    .forEach(p -> {
                        try {
                            String inputFile = p.toString();
                            String outputFile = Paths.get(outDirPath, p.getFileName().toString().replace(".csv.gz", ".cboe.trades.csv.zst")).toString();
                            new RAW2Trades().processGzFile(inputFile, outputFile, "EURUSD", "CBOE");
                        } catch (IOException e) {
                            System.err.println("An error occurred during processing file " + p + ": " + e.getMessage());
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            System.err.println("An error occurred walking the input directory: " + e.getMessage());
            e.printStackTrace();
        }
    }
}