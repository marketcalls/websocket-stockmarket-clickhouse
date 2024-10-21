# WebSocket Stock Market Data to ClickHouse

This project demonstrates how to capture real-time stock market data via WebSocket and store it efficiently in ClickHouse, a column-oriented database designed for high-performance analytics.

## Features

- Real-time data capture from stock market WebSocket feeds
- Efficient storage in ClickHouse database
- Scalable architecture for handling high-frequency trading data
- Python-based implementation with asyncio for concurrent operations

## Prerequisites

- Python 3.7+
- ClickHouse database
- Access to a stock market WebSocket feed

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/marketcalls/websocket-stockmarket-clickhouse.git
   cd websocket-stockmarket-clickhouse
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up your ClickHouse database and configure the connection details in the `.env` file.

## Usage

Run the main script to start capturing and storing data:

```
python main.py
```

## Configuration

Edit the `.env` file to set your WebSocket feed URL, ClickHouse connection details, and other configuration parameters.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by [Ravindra Elicherla's article on Medium](https://ravindraelicherla.medium.com/storing-tick-by-tick-webscocket-data-into-clickhouse-f4bbd29d0d65)