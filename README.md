# quant

## Schema Definition

The data is stored in a CSV format and contains the following fields:

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



# Normalized Trades CSV Format

This section describes the structure of the final normalized trades CSV file. This format is designed to be clean, standardized, and easy to use for analysis.

| Column Name            | Arrow Data Type                 | Description                                                                                |
|:-----------------------|:--------------------------------|:-------------------------------------------------------------------------------------------|
| `timestamp`            | `Utf8`                          | The UTC timestamp of the trade in ISO 8601 format (`YYYY-MM-DDTHH:MM:SS.sssZ`).              |
| `timestamp_millis_utc` | `Timestamp(millisecond, "UTC")` | The UTC timestamp of the trade as the number of milliseconds since the Unix epoch.         |
| `tx_px`                | `Int64`                         | The execution price of the trade. This is a scaled integer.                                |
| `tx_sz`                | `Int64`                         | The size or quantity of the trade.                                                         |
| `tx_agg_side`          | `Dictionary(Int8, Utf8)`        | The aggressor side of the trade, indicating the initiator. `B` for Buy, `S` for Sell.        |
| `sym`                  | `Dictionary(Int8, Utf8)`        | The trading symbol for the instrument (e.g., `EURUSD`).                                    |
| `source`               | `Dictionary(Int8, Utf8)`        | The exchange or venue where the trade occurred (e.g., `EURONEXT`, `CBOE`).                 |