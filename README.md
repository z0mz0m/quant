# quant

## LOB Schema Definition

The data is stored in the following format and contains the fields:

| Field Name           | Arrow Data Type                 | Description                                                                                                                                                             |
|:---------------------|:--------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `timestamp`          | `Utf8`                          | A human-readable UTC timestamp (`YYYY-MM-DDTHH:mm:ss.sssZ`) indicating when the LOB snapshot was taken.                                                                  |
| `timestamp_millis_utc` | `Timestamp(millisecond, "UTC")` | The same timestamp represented as Unix milliseconds since the UTC epoch.                                                                                                |
| `bids_cnt`           | `Int32`                         | The total number of individual bid orders in the book at that moment.                                                                                                   |
| `asks_cnt`           | `Int32`                         | The total number of individual ask (or offer) orders in the book at that moment.                                                                                        |
| `total_pages`        | `Int32`                         | The total number of pages for a snapshot if the LOB is split across multiple records.                                                                                   |
| `page_index`         | `Int32`                         | The index of the current data page (0-based). For a full snapshot in one row, this is `0`.                                                                              |
| `sum_lhs_bid_sz`     | `Int64`                         | **Left-Hand Side Bid Size**: The sum of all bid order sizes. For a pair like `EUR/USD`, this is the total amount of the base currency (EUR).                              |
| `sum_rhs_bid_sz`     | `Int64`                         | **Right-Hand Side Bid Size**: The total notional value of all bid orders, calculated as `SUM(price * size)`. For `EUR/USD`, this would be the value in the quote currency (USD). |
| `sum_lhs_ask_sz`     | `Int64`                         | **Left-Hand Side Ask Size**: The sum of all ask order sizes. For `EUR/USD`, this is the total amount of the base currency (EUR).                                         |
| `sum_rhs_ask_sz`     | `Int64`                         | **Right-Hand Side Ask Size**: The total notional value of all ask orders, calculated as `SUM(price * size)`. For `EUR/USD`, this is the value in the quote currency (USD). |
| `bid_px_00` to `_09` | `Int64`                         | The prices for the 10 best bid levels in the book. `bid_px_00` is the highest bid price. **Prices are scaled by 10<sup>9</sup>.**                                     |
| `bid_sz_00` to `_09` | `Int64`                         | The total volume (size) available at each of the 10 corresponding bid price levels.                                                                                    |
| `ask_px_00` to `_09` | `Int64`                         | The prices for the 10 best ask levels in the book. `ask_px_00` is the lowest ask price. **Prices are scaled by 10<sup>9</sup>.**                                       |
| `ask_sz_00` to `_09` | `Int64`                         | The total volume (size) available at each of the 10 corresponding ask price levels.                                                                                    |
| `sym`                | `Dictionary(Int8, Utf8)`        | The symbol for the financial instrument (e.g., `EURUSD`).                                                                                                               |
| `source`             | `Dictionary(Int8, Utf8)`        | The exchange or data provider where the data originated (e.g., `CBOE`, `EURONEXT`).                                                                                     |                                                                                    |

### Example


timestamp,timestamp_millis_utc,bids_cnt,asks_cnt,total_pages,page_index,sum_lhs_bid_sz,sum_rhs_bid_sz,sum_lhs_ask_sz,sum_rhs_ask_sz,

bid_px_00,bid_sz_00,ask_px_00,ask_sz_00,bid_px_01,bid_sz_01,ask_px_01,ask_sz_01,bid_px_02,bid_sz_02,ask_px_02,ask_sz_02,

bid_px_03,bid_sz_03,ask_px_03,ask_sz_03,bid_px_04,bid_sz_04,ask_px_04,ask_sz_04,bid_px_05,bid_sz_05,ask_px_05,ask_sz_05,

bid_px_06,bid_sz_06,ask_px_06,ask_sz_06,bid_px_07,bid_sz_07,ask_px_07,ask_sz_07,bid_px_08,bid_sz_08,ask_px_08,ask_sz_08,

bid_px_09,bid_sz_09,ask_px_09,ask_sz_09,sym,source

2025-06-29T21:03:13.133Z,1751230993133,1,1,1,0,1000000,1172870,1000000,1173320,1172870000,1000000,1173320000,1000000,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,EURUSD,CBOE



# Normalized Trades  Format

This section describes the structure of the final normalized trades  file. This format is designed to be clean, standardized, and easy to use for analysis.

| Column Name            | Arrow Data Type                 | Description                                                                                |
|:-----------------------|:--------------------------------|:-------------------------------------------------------------------------------------------|
| `timestamp`            | `Utf8`                          | The UTC timestamp of the trade in ISO 8601 format (`YYYY-MM-DDTHH:MM:SS.sssZ`).              |
| `timestamp_millis_utc` | `Timestamp(millisecond, "UTC")` | The UTC timestamp of the trade as the number of milliseconds since the Unix epoch.         |
| `sub_ms_idx`           | `UInt8`                         | A 0-based index to preserve the order of trades that occur within the same millisecond.    |
| `scaled_tx_px`         | `Int64`                         | The execution price of the trade. This is a scaled integer.                                |
| `tx_sz`                | `Int64`                         | The size or quantity of the trade.                                                         |
| `tx_agg_side`          | `Dictionary(Int8, Utf8)`        | The aggressor side of the trade, indicating the initiator. `B` for Buy, `S` for Sell.        |
| `sym`                  | `Dictionary(Int8, Utf8)`        | The trading symbol for the instrument (e.g., `EURUSD`).                                    |
| `source`               | `Dictionary(Int8, Utf8)`        | The exchange or venue where the trade occurred (e.g., `EURONEXT`, `CBOE`).                 |



# JVM parameters

--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED

# Note on nanoarrow /arrow community extension for duckdb 

https://github.com/paleolimbot/duckdb-nanoarrow?tab=readme-ov-file

Arrow IPC files (.arrow) and Arrow IPC streams (.arrows) are distinct but related formats. This extension can read both but only writes Arrow IPC Streams.

# Note on clickhouse arrow/arrows 

https://clickhouse.com/docs/interfaces/formats/Arrow

Note on defaults, including usage of LD4 as default
also one cannot set the size of record batches inside the arrow 


# Note how to run 

# several batches processed :

java --add-modules jdk.incubator.vector  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp .\target\quant-1.0-SNAPSHOT.jar arrow.OffHeapReader


Performance Breakdown:
- Setup & Initialization:          5.792 ms
- Reading & Calculation (Total):  12.328 ms
  |-- Disk I/O (Load Batch):       8.378 ms
  |-- Zero-Copy Vector Calc:       1.648 ms
  |-- Logic Overhead:              2.303 ms
- Parallel Sort:                   3.478 ms
-----------------------------------------
Total Execution Time:               16.108 ms



# one batch :

java  --add-modules jdk.incubator.vector  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp .\target\quant-1.0-SNAPSHOT.jar arrows.ArrowSTradeVectorReaderOffHeap


Percentiles (time in milliseconds):
- 25.0th percentile (0.25): <= 205 ms
- 50.0th percentile (0.50): <= 3392 ms
- 75.0th percentile (0.75): <= 12590 ms
- 90.0th percentile (0.90): <= 33556 ms
- 95.0th percentile (0.95): <= 60014 ms
- 99.0th percentile (0.99): <= 167033 ms
- 99.9th percentile (1.00): <= 519973 ms
- 100.0th percentile (1.00): <= 176483523 ms
-----------------------------------------
Performance Breakdown:
- Setup & Initialization:      0.517 ms
- Reading & Delta Calculation: 5.640 ms
  - I/O (Loading Batches):     3.912 ms
  - Off-Heap Segment Setup:    0.017 ms
  - Vector API Delta Calc:     1.675 ms
- List to Array Conversion:    0.437 ms
- Parallel Sort:               1.504 ms
-----------------------------------------
Total time to read and compute CDF: 8.233 milliseconds
-----------------------------------------


# lz4 compressed

Opened Arrow file: data/cboe/normalized/EDF_OUTPUT_NY_20251205.threesixtyt.lob.clickhouse.lz4.arrow
Processing 1544 batches...
Finished reading 95452199 trades.
-----------------------------------------
Analysis Results: Inter-Trade Arrival Time CDF
Total calculated time differences: 95,452,198
Percentiles (ms):
-  25.0%: <= 0 ms
-  50.0%: <= 0 ms
-  75.0%: <= 100 ms
-  90.0%: <= 100 ms
-  95.0%: <= 137 ms
-  99.0%: <= 433 ms
-  99.9%: <= 1002 ms
- 100.0%: <= 38546290 ms
-----------------------------------------
this should be zero:2.0E-4
Performance Breakdown:
- Setup & Initialization:        323.128 ms
- Reading & Calculation (Total):114150.903 ms
  |-- Disk I/O (Load Batch):    113091.275 ms
  |-- Zero-Copy Vector Calc:     108.776 ms
  |-- Logic Overhead:            950.852 ms
- Parallel Sort:                 514.618 ms
-----------------------------------------
Total Execution Time:             114667.050 ms
-----------------------------------------


# a lot larger file 95_452_199 rows


--- Iteration 5 ---
Successfully opened Arrow stream file: data/cboe/normalized/EDF_OUTPUT_NY_20251205.threesixtyt.lob.clickhouse.nocompression.arrow
Schema: Schema<timestamp_millis_utc: Int(64, true), sym: Utf8, source: Utf8>
-----------------------------------------
Calculating inter-trade arrival times using Vector API and Off-Heap Memory...
Finished reading 95452199 trades.
-----------------------------------------
Analysis Results: Inter-Trade Arrival Time CDF
Total calculated time differences: 95,452,198
Percentiles (time in milliseconds):
- 25.0th percentile (0.25): <= 0 ms
- 50.0th percentile (0.50): <= 0 ms
- 75.0th percentile (0.75): <= 100 ms
- 90.0th percentile (0.90): <= 100 ms
- 95.0th percentile (0.95): <= 137 ms
- 99.0th percentile (0.99): <= 433 ms
- 99.9th percentile (1.00): <= 1002 ms
- 100.0th percentile (1.00): <= 54174465 ms
-----------------------------------------
Performance Breakdown:
- Setup & Initialization:      0.953 ms
- Reading & Delta Calculation: 3962.109 ms
  - I/O (Loading Batches):     2991.581 ms
  - Off-Heap Segment Setup:    0.481 ms
  - Vector API Delta Calc:     968.383 ms
- List to Array Conversion:    264.262 ms
- Parallel Sort:               297.129 ms
-----------------------------------------
Total time to read and compute CDF: 4524.861 milliseconds


# WSL2 setup

sudo nice -n -20 java --enable-native-access=ALL-UNNAMED -Xmx4G -Xms4G -XX:+UseParallelGC  -XX:+UseLargePages -XX:+AlwaysPreTouch -XX:MaxDirectMemorySize=12G --enable-preview --add-modules jdk.incubator.vector  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp target/quant-1.0-SNAPSHOT.jar arrows.ArrowsOffHeap


Successfully opened Arrow stream file: data/cboe/normalized/EDF_OUTPUT_NY_20251205.threesixtyt.lob.arrows
Schema: Schema<timestamp_millis_utc: Int(64, true), sym: Utf8, source: Utf8>
-----------------------------------------
Calculating inter-trade arrival times using Vector API and Off-Heap Memory...
Finished reading 95452199 trades.
-----------------------------------------
Analysis Results: Inter-Trade Arrival Time CDF
Total calculated time differences: 95,451,427
Percentiles (time in milliseconds):
- 25.0th percentile (0.25): <= 0 ms
- 50.0th percentile (0.50): <= 0 ms
- 75.0th percentile (0.75): <= 0 ms
- 90.0th percentile (0.90): <= 100 ms
- 95.0th percentile (0.95): <= 105 ms
- 99.0th percentile (0.99): <= 374 ms
- 99.9th percentile (1.00): <= 1001 ms
- 100.0th percentile (1.00): <= 79199994 ms
-----------------------------------------
Performance Breakdown:
- Setup & Initialization:      337.238 ms
- Reading & Delta Calculation: 884.961 ms
  - I/O (Loading Batches):     756.292 ms
  - Off-Heap Segment Setup:    0.372 ms
  - Vector API Delta Calc:     127.126 ms
- List to Array Conversion:    173.403 ms
- Parallel Sort:               280.732 ms
-----------------------------------------
Total time to read and compute CDF: 1676.428 milliseconds



# Parallel load of data 


sudo chrt -f 99 taskset -c 0-7 java --enable-native-access=ALL-UNNAMED -Xmx4G -Xms4G -XX:+UseParallelGC  -XX:+UseLargePages -XX:+AlwaysPreTouch -XX:MaxDirectMemorySize=12G --enable-preview --add-modules jdk.incubator.vector  --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -cp target/quant-1.0-SNAPSHOT.jar arrow.ArrowOffHeapParallelLoad



--- Iteration 3 ---
Performance Breakdown:
- Metadata Scan:       0.317 ms
- Parallel Read/Calc:  171.627 ms
- Sorting:             196.587 ms
- Total Time:          530.247 ms
-----------------------------------------
Analysis Results: Inter-Trade Arrival Time CDF
Total calculated time differences: 95,450,655
Percentiles (time in milliseconds):
- 25.0th percentile (0.25): <= 0 ms
- 50.0th percentile (0.50): <= 0 ms
- 75.0th percentile (0.75): <= 100 ms
- 90.0th percentile (0.90): <= 100 ms
- 95.0th percentile (0.95): <= 137 ms
- 99.0th percentile (0.99): <= 433 ms
- 99.9th percentile (1.00): <= 1002 ms
- 100.0th percentile (1.00): <= 14999 ms
-----------------------------------------


--- Iteration 2 ---
Performance Breakdown:
- Metadata Scan:       0.545 ms
- Parallel Read/Calc:  197.496 ms
- Sorting:             159.128 ms
- Total Time:          514.494 ms
-----------------------------------------
Analysis Results: Inter-Trade Arrival Time CDF
Total calculated time differences: 95,450,655
Percentiles (time in milliseconds):
- 25.0th percentile (0.25): <= 0 ms
- 50.0th percentile (0.50): <= 0 ms
- 75.0th percentile (0.75): <= 100 ms
- 90.0th percentile (0.90): <= 100 ms
- 95.0th percentile (0.95): <= 137 ms
- 99.0th percentile (0.99): <= 433 ms
- 99.9th percentile (1.00): <= 1002 ms
- 100.0th percentile (1.00): <= 14999 ms
-----------------------------------------



# further things ...
vmtouch -v -t  data/cboe/normalized/EDF_OUTPUT_NY_20251205.threesixtyt.lob.clickhouse.nocompression.arrow