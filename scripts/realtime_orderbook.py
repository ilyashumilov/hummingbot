#!/usr/bin/env python3
"""
Real-time Orderbook Monitor Strategy
Fetches and displays real-time orderbook data for a given connector and symbol.
Can be run as a standalone script or through the Hummingbot API Gateway.
"""

import asyncio
import json
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Any
from datetime import datetime

from hummingbot.strategy.script_strategy_base import ScriptStrategyBase
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_row import OrderBookRow


class RealtimeOrderbookConfig:
    """Configuration for the Real-time Orderbook Monitor"""
    
    # Main Parameters
    connector: str = "binance_paper_trade"
    trading_pair: str = "ETH-USDT" 
    refresh_interval: float = 1.0  # seconds
    orderbook_depth: int = 10  # number of levels to display
    connector_type: str = "spot"  # "spot" or "perpetual"
    
    # Display Options
    show_volumes: bool = True
    show_spreads: bool = True
    show_mid_price: bool = True
    show_timestamps: bool = True
    compact_display: bool = False
    
    # Filtering Options
    min_volume_filter: Optional[float] = None  # minimum volume to display
    price_precision: int = 4
    volume_precision: int = 4
    
    # Output Options
    log_to_file: bool = True
    json_output: bool = False
    csv_output: bool = False
    output_file: Optional[str] = None
    
    # Alert Options
    spread_alert_threshold: Optional[float] = None  # alert if spread exceeds this %
    volume_alert_threshold: Optional[float] = None  # alert if total volume drops below this

    def update_from_dict(self, config_dict: Dict[str, Any]):
        """Update configuration from dictionary (for API usage)"""
        for key, value in config_dict.items():
            if hasattr(self, key):
                setattr(self, key, value)


class RealtimeOrderbook(ScriptStrategyBase):
    """
    Real-time orderbook monitoring strategy.
    Continuously fetches and displays orderbook data with customizable formatting.
    """
    
    def __init__(self, config: Optional[RealtimeOrderbookConfig] = None):
        """Initialize the orderbook monitor"""
        super().__init__()
        self.config = config or RealtimeOrderbookConfig()
        
        # Auto-detect connector type if not specified
        self._detect_connector_type()
        
        # Set up markets based on config and connector type
        if self.config.connector_type == "perpetual":
            self.derivative_markets = {
                self.config.connector: {self.config.trading_pair}
            }
            self.markets = {}  # No spot markets for perpetual
        else:
            self.markets = {
                self.config.connector: {self.config.trading_pair}
            }
        
        # Internal state
        self.last_update_time = None
        self.update_count = 0
        self.is_running = True
        self.orderbook_history = []
        
        # Setup logging
        self.setup_logging()
        
    def setup_logging(self):
        """Setup logging configuration"""
        if self.config.log_to_file and self.config.output_file:
            file_handler = logging.FileHandler(self.config.output_file)
            file_handler.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            self.logger().addHandler(file_handler)

    def _detect_connector_type(self):
        """Auto-detect if connector is perpetual or spot based on name"""
        perpetual_keywords = [
            'perpetual', 'perp', 'futures', 'derivative', 'dydx', 'hyperliquid',
            'binance_perpetual', 'bybit_perpetual', 'okx_perpetual', 'kucoin_perpetual'
        ]
        
        connector_lower = self.config.connector.lower()
        
        # Check if connector type was explicitly set
        if hasattr(self.config, 'connector_type') and self.config.connector_type:
            return
        
        # Auto-detect based on connector name
        for keyword in perpetual_keywords:
            if keyword in connector_lower:
                self.config.connector_type = "perpetual"
                return
        
        # Default to spot
        self.config.connector_type = "spot"
    
    def update_config(self, new_config: Dict[str, Any]):
        """Update configuration dynamically (for API usage)"""
        self.config.update_from_dict(new_config)
        
        # Re-detect connector type if connector changed
        if 'connector' in new_config:
            self._detect_connector_type()
        
        # Update markets if connector or trading pair changed
        if 'connector' in new_config or 'trading_pair' in new_config or 'connector_type' in new_config:
            if self.config.connector_type == "perpetual":
                self.derivative_markets = {
                    self.config.connector: {self.config.trading_pair}
                }
                self.markets = {}
            else:
                self.markets = {
                    self.config.connector: {self.config.trading_pair}
                }
                if hasattr(self, 'derivative_markets'):
                    self.derivative_markets = {}
            
            self.logger().info(f"Updated monitor config: {self.config.connector} ({self.config.connector_type}) - {self.config.trading_pair}")

    def on_tick(self):
        """Main execution loop - called regularly by Hummingbot"""
        if not self.is_running:
            return
            
        try:
            self.fetch_and_display_orderbook()
        except Exception as e:
            self.logger().error(f"Error in orderbook fetch: {e}")

    def fetch_and_display_orderbook(self):
        """Fetch and display current orderbook data"""
        # Get connector from appropriate collection
        if self.config.connector_type == "perpetual":
            connector = self.derivative_connectors.get(self.config.connector) if hasattr(self, 'derivative_connectors') else None
        else:
            connector = self.connectors.get(self.config.connector)
        
        if not connector:
            self.logger().error(f"Connector {self.config.connector} ({self.config.connector_type}) not found")
            return

        # Get orderbook
        try:
            orderbook = connector.get_order_book(self.config.trading_pair)
        except Exception as e:
            self.logger().error(f"Failed to get orderbook from {self.config.connector}: {e}")
            return
            
        if not orderbook:
            self.logger().warning(f"No orderbook data available for {self.config.trading_pair} on {self.config.connector}")
            return

        # Process orderbook data
        orderbook_data = self.process_orderbook(orderbook)
        
        # Display based on output format
        if self.config.json_output:
            self.display_json_format(orderbook_data)
        elif self.config.csv_output:
            self.display_csv_format(orderbook_data)
        else:
            self.display_table_format(orderbook_data)
            
        # Check alerts
        self.check_alerts(orderbook_data)
        
        # Update counters
        self.update_count += 1
        self.last_update_time = datetime.now()
        
        # Store history if needed
        if len(self.orderbook_history) > 100:  # Keep last 100 updates
            self.orderbook_history.pop(0)
        self.orderbook_history.append(orderbook_data)

    def process_orderbook(self, orderbook: OrderBook) -> Dict[str, Any]:
        """Process raw orderbook data into structured format"""
        # Get bids and asks
        bids = orderbook.snapshot[0]  # List of OrderBookRow objects
        asks = orderbook.snapshot[1]  # List of OrderBookRow objects
        
        # Process bids (buy orders) - sort by price descending
        processed_bids = []
        for bid in sorted(bids.values(), key=lambda x: x.price, reverse=True)[:self.config.orderbook_depth]:
            if self.config.min_volume_filter and bid.amount < self.config.min_volume_filter:
                continue
            processed_bids.append({
                'price': round(float(bid.price), self.config.price_precision),
                'volume': round(float(bid.amount), self.config.volume_precision),
                'total': round(float(bid.price * bid.amount), self.config.volume_precision)
            })
        
        # Process asks (sell orders) - sort by price ascending
        processed_asks = []
        for ask in sorted(asks.values(), key=lambda x: x.price)[:self.config.orderbook_depth]:
            if self.config.min_volume_filter and ask.amount < self.config.min_volume_filter:
                continue
            processed_asks.append({
                'price': round(float(ask.price), self.config.price_precision),
                'volume': round(float(ask.amount), self.config.volume_precision),
                'total': round(float(ask.price * ask.amount), self.config.volume_precision)
            })
        
        # Calculate metrics
        best_bid = processed_bids[0]['price'] if processed_bids else 0
        best_ask = processed_asks[0]['price'] if processed_asks else 0
        mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
        spread = best_ask - best_bid if best_bid and best_ask else 0
        spread_percent = (spread / mid_price * 100) if mid_price > 0 else 0
        
        # Calculate total volumes
        total_bid_volume = sum(bid['volume'] for bid in processed_bids)
        total_ask_volume = sum(ask['volume'] for ask in processed_asks)
        
        return {
            'timestamp': datetime.now(),
            'connector': self.config.connector,
            'trading_pair': self.config.trading_pair,
            'bids': processed_bids,
            'asks': processed_asks,
            'best_bid': best_bid,
            'best_ask': best_ask,
            'mid_price': mid_price,
            'spread': spread,
            'spread_percent': spread_percent,
            'total_bid_volume': total_bid_volume,
            'total_ask_volume': total_ask_volume,
            'update_count': self.update_count
        }

    def display_table_format(self, data: Dict[str, Any]):
        """Display orderbook in table format"""
        if self.config.compact_display:
            self.display_compact_table(data)
        else:
            self.display_full_table(data)

    def display_full_table(self, data: Dict[str, Any]):
        """Display full orderbook table"""
        separator = "=" * 80
        
        # Header
        header_info = []
        if self.config.show_timestamps:
            header_info.append(f"Time: {data['timestamp'].strftime('%H:%M:%S.%f')[:-3]}")
        header_info.append(f"Pair: {data['trading_pair']}")
        header_info.append(f"Exchange: {data['connector']}")
        
        self.logger().info(separator)
        self.logger().info(f"ORDERBOOK - {' | '.join(header_info)}")
        
        # Market metrics
        if self.config.show_mid_price or self.config.show_spreads:
            metrics_info = []
            if self.config.show_mid_price:
                metrics_info.append(f"Mid: ${data['mid_price']:.{self.config.price_precision}f}")
            if self.config.show_spreads:
                metrics_info.append(f"Spread: ${data['spread']:.{self.config.price_precision}f} ({data['spread_percent']:.2f}%)")
            self.logger().info(f"METRICS: {' | '.join(metrics_info)}")
        
        # Asks (sell orders) - display top to bottom (highest to lowest)
        self.logger().info(f"\nASKS (Sell Orders) - Total Volume: {data['total_ask_volume']:.{self.config.volume_precision}f}")
        self.logger().info("Price" + " " * (self.config.price_precision + 8) + "Volume" + " " * (self.config.volume_precision + 6) + "Total")
        self.logger().info("-" * 60)
        
        for ask in reversed(data['asks'][-5:]):  # Show top 5 asks
            price_str = f"${ask['price']:.{self.config.price_precision}f}"
            volume_str = f"{ask['volume']:.{self.config.volume_precision}f}" if self.config.show_volumes else ""
            total_str = f"${ask['total']:.2f}"
            self.logger().info(f"{price_str:<15} {volume_str:<15} {total_str}")
        
        # Spread indicator
        self.logger().info(f"{'─' * 30} SPREAD ${data['spread']:.{self.config.price_precision}f} {'─' * 30}")
        
        # Bids (buy orders) - display top to bottom (highest to lowest)
        self.logger().info(f"BIDS (Buy Orders) - Total Volume: {data['total_bid_volume']:.{self.config.volume_precision}f}")
        self.logger().info("Price" + " " * (self.config.price_precision + 8) + "Volume" + " " * (self.config.volume_precision + 6) + "Total")
        self.logger().info("-" * 60)
        
        for bid in data['bids'][:5]:  # Show top 5 bids
            price_str = f"${bid['price']:.{self.config.price_precision}f}"
            volume_str = f"{bid['volume']:.{self.config.volume_precision}f}" if self.config.show_volumes else ""
            total_str = f"${bid['total']:.2f}"
            self.logger().info(f"{price_str:<15} {volume_str:<15} {total_str}")
            
        self.logger().info(separator)

    def display_compact_table(self, data: Dict[str, Any]):
        """Display compact orderbook format"""
        timestamp = data['timestamp'].strftime('%H:%M:%S')
        self.logger().info(
            f"{timestamp} | {data['trading_pair']} | "
            f"Bid: ${data['best_bid']:.{self.config.price_precision}f} | "
            f"Ask: ${data['best_ask']:.{self.config.price_precision}f} | "
            f"Mid: ${data['mid_price']:.{self.config.price_precision}f} | "
            f"Spread: {data['spread_percent']:.2f}%"
        )

    def display_json_format(self, data: Dict[str, Any]):
        """Display orderbook in JSON format"""
        # Convert datetime to string for JSON serialization
        json_data = data.copy()
        json_data['timestamp'] = data['timestamp'].isoformat()
        
        json_output = json.dumps(json_data, indent=2)
        self.logger().info(json_output)

    def display_csv_format(self, data: Dict[str, Any]):
        """Display orderbook in CSV format"""
        timestamp = data['timestamp'].isoformat()
        
        # Header (only on first update)
        if self.update_count == 1:
            self.logger().info("timestamp,connector,trading_pair,best_bid,best_ask,mid_price,spread,spread_percent,total_bid_volume,total_ask_volume")
        
        # Data row
        csv_row = f"{timestamp},{data['connector']},{data['trading_pair']},{data['best_bid']},{data['best_ask']},{data['mid_price']},{data['spread']},{data['spread_percent']},{data['total_bid_volume']},{data['total_ask_volume']}"
        self.logger().info(csv_row)

    def check_alerts(self, data: Dict[str, Any]):
        """Check for alert conditions"""
        alerts = []
        
        if self.config.spread_alert_threshold and data['spread_percent'] > self.config.spread_alert_threshold:
            alerts.append(f"ALERT: High spread: {data['spread_percent']:.2f}% (threshold: {self.config.spread_alert_threshold}%)")
        
        if self.config.volume_alert_threshold:
            total_volume = data['total_bid_volume'] + data['total_ask_volume']
            if total_volume < self.config.volume_alert_threshold:
                alerts.append(f"ALERT: Low volume: {total_volume:.4f} (threshold: {self.config.volume_alert_threshold})")
        
        for alert in alerts:
            self.logger().warning(alert)

    def get_current_data(self) -> Optional[Dict[str, Any]]:
        """Get current orderbook data (for API access)"""
        if self.orderbook_history:
            return self.orderbook_history[-1]
        return None

    def get_orderbook_snapshot(self) -> Dict[str, Any]:
        """Get current orderbook snapshot for API"""
        # Get connector from appropriate collection
        if self.config.connector_type == "perpetual":
            connector = self.derivative_connectors.get(self.config.connector) if hasattr(self, 'derivative_connectors') else None
        else:
            connector = self.connectors.get(self.config.connector)
        
        if not connector:
            return {"error": f"Connector {self.config.connector} ({self.config.connector_type}) not found"}

        try:
            orderbook = connector.get_order_book(self.config.trading_pair)
        except Exception as e:
            return {"error": f"Failed to get orderbook: {str(e)}"}
            
        if not orderbook:
            return {"error": f"No orderbook data available for {self.config.trading_pair} on {self.config.connector}"}

        return self.process_orderbook(orderbook)

    def stop_monitoring(self):
        """Stop the orderbook monitoring"""
        self.is_running = False
        self.logger().info("Orderbook monitoring stopped")

    def format_status(self) -> str:
        """Format status for Hummingbot display"""
        if not self.last_update_time:
            return "Initializing orderbook monitor..."
        
        return (
            f"Orderbook Monitor - {self.config.trading_pair} on {self.config.connector} ({self.config.connector_type})\n"
            f"Updates: {self.update_count} | Last: {self.last_update_time.strftime('%H:%M:%S')}\n"
            f"Refresh: {self.config.refresh_interval}s | Depth: {self.config.orderbook_depth} levels"
        )


# Global instance for API access
_orderbook_monitor = None

def get_orderbook_monitor() -> Optional[RealtimeOrderbook]:
    """Get the global orderbook monitor instance"""
    global _orderbook_monitor
    return _orderbook_monitor

def create_orderbook_monitor(config: Dict[str, Any]) -> RealtimeOrderbook:
    """Create and configure orderbook monitor for API usage"""
    global _orderbook_monitor
    
    monitor_config = RealtimeOrderbookConfig()
    monitor_config.update_from_dict(config)
    
    _orderbook_monitor = RealtimeOrderbook(monitor_config)
    return _orderbook_monitor

def get_supported_connectors() -> Dict[str, List[str]]:
    """Get list of supported connectors by type"""
    return {
        "spot": [
            "binance", "binance_paper_trade",
            "kucoin", "kucoin_paper_trade", 
            "gate_io", "gate_io_paper_trade",
            "okx", "okx_paper_trade",
            "bybit", "bybit_paper_trade",
            "coinbase_pro", "coinbase_advanced_trade",
            "kraken", "huobi", "bitfinex", "bittrex"
        ],
        "perpetual": [
            "binance_perpetual", "binance_perpetual_testnet",
            "bybit_perpetual", "bybit_perpetual_testnet",
            "okx_perpetual", "okx_perpetual_testnet",
            "kucoin_perpetual", "kucoin_perpetual_testnet",
            "dydx_perpetual", "hyperliquid_perpetual",
            "gate_io_perpetual", "bitget_perpetual"
        ]
    }

def main():
    """Standalone execution for testing"""
    import argparse
    
    supported_connectors = get_supported_connectors()
    all_connectors = supported_connectors["spot"] + supported_connectors["perpetual"]
    
    parser = argparse.ArgumentParser(description="Real-time Orderbook Monitor")
    parser.add_argument("--connector", default="binance_paper_trade", 
                       help=f"Exchange connector. Supported: {', '.join(all_connectors[:10])}...")
    parser.add_argument("--pair", default="ETH-USDT", help="Trading pair (e.g., ETH-USDT, BTC-USD-PERP)")
    parser.add_argument("--type", choices=["spot", "perpetual"], default="auto", 
                       help="Connector type (auto-detect if not specified)")
    parser.add_argument("--interval", type=float, default=1.0, help="Refresh interval in seconds")
    parser.add_argument("--depth", type=int, default=10, help="Orderbook depth")
    parser.add_argument("--compact", action="store_true", help="Compact display mode")
    parser.add_argument("--json", action="store_true", help="JSON output format")
    parser.add_argument("--csv", action="store_true", help="CSV output format")
    parser.add_argument("--list-connectors", action="store_true", help="List supported connectors")
    
    args = parser.parse_args()
    
    # List connectors if requested
    if args.list_connectors:
        print("Supported Connectors:")
        print("\nSpot Connectors:")
        for connector in supported_connectors["spot"]:
            print(f"  - {connector}")
        print("\nPerpetual Connectors:")
        for connector in supported_connectors["perpetual"]:
            print(f"  - {connector}")
        return
    
    # Create config
    config = RealtimeOrderbookConfig()
    config.connector = args.connector
    config.trading_pair = args.pair
    config.refresh_interval = args.interval
    config.orderbook_depth = args.depth
    config.compact_display = args.compact
    config.json_output = args.json
    config.csv_output = args.csv
    
    # Set connector type if specified
    if args.type != "auto":
        config.connector_type = args.type
    
    # Validate connector
    if args.connector not in all_connectors:
        print(f"Warning: {args.connector} may not be supported")
        print(f"Use --list-connectors to see supported connectors")
    
    print(f"Starting orderbook monitor for {args.pair} on {args.connector}")
    print(f"Connector type: {getattr(config, 'connector_type', 'auto-detect')}")
    print(f"Refresh interval: {args.interval}s | Depth: {args.depth} levels")
    print("Press Ctrl+C to stop\n")
    
    # Create and run strategy
    strategy = RealtimeOrderbook(config)
    
    try:
        # This is a simplified standalone version
        # In practice, this would be run within Hummingbot's framework
        while True:
            strategy.fetch_and_display_orderbook()
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nStopping orderbook monitor...")
        strategy.stop_monitoring()


if __name__ == "__main__":
    import time
    main()
