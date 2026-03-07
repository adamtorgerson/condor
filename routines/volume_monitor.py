"""Monitor volume and alert on threshold."""

import asyncio
import logging
import time
import datetime
from pydantic import BaseModel, Field
from telegram.ext import ContextTypes

from config_manager import get_client
from utils.telegram_formatters import escape_markdown_v2

logger = logging.getLogger(__name__)

# Mark as continuous routine - has internal loop
CONTINUOUS = True


class Config(BaseModel):
    """Live volume monitor with configurable alerts."""

    connector: str = Field(default="binance", description="CEX connector name")
    trading_pair: str = Field(default="BTC-USDT", description="Trading pair to monitor")
    threshold_vol: float = Field(default=1.0, description="Alert threshold volume")
    interval_sec: int = Field(default=10, description="Check interval in seconds")


async def run(config: Config, context: ContextTypes.DEFAULT_TYPE) -> str:
    """
    Monitor volume continuously.

    This is a continuous routine - runs forever until cancelled.
    Sends alert messages when threshold is crossed.
    """
    chat_id = context._chat_id if hasattr(context, '_chat_id') else None
    instance_id = getattr(context, '_instance_id', 'default')

    client = await get_client(chat_id, context=context)
    await client.init()
    if not client:
        return "No server available"

    # State for tracking
    state = {
        "alerts_sent": 0,
        "updates": 0,
        "start_time": time.time(),
    }

    # Send start notification
    try:
        pair_esc = escape_markdown_v2(config.trading_pair)
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"🟢 *Volume Monitor Started*\n{pair_esc} @ {escape_markdown_v2(config.connector)}",
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        logger.error(f"Failed to send start message: {e}")

    try:
        # Main monitoring loop
        while True:
            try:
                await client.init()
                # Fetch candles and current price
                candles = await client.market_data.get_candles(
                    connector_name=config.connector,
                    trading_pair=config.trading_pair,
                    interval="5m",
                    max_records=1
                )
                current_volume = candles[0].get("volume")
                candle_time = datetime.date.fromtimestamp(candles[0].get("timestamp"))
                if not current_volume:
                    await asyncio.sleep(config.interval_sec)
                    continue
        
                # Get current price
                prices = await client.market_data.get_prices(
                    connector_name=config.connector,
                    trading_pairs=config.trading_pair
                )
                current_price = prices["prices"].get(config.trading_pair)
                
                if not current_price:
                    await asyncio.sleep(config.interval_sec)
                    continue

                # Update tracking
                state["updates"] = state["updates"] + 1

                # Check threshold for alert
                if abs(current_volume) >= config.threshold_vol:
                    pair_esc = escape_markdown_v2(config.trading_pair)
                    thresh_esc = escape_markdown_v2(config.threshold_vol)
                    price_esc = escape_markdown_v2(f"${current_price:,.2f}")
                    volume_esc = escape_markdown_v2(f"{current_volume:.2f}")
                    time_esc = escape_markdown_v2(candle_time)
                    
                    try:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"Volume *{pair_esc} Alert `{thresh_esc}`*\n"
                                f"Volume: `{volume_esc}`\n"
                                f"Candle Time: `{time_esc}`\n"
                                f"Price: `{price_esc}`"
                            ),
                            parse_mode="MarkdownV2"
                        )
                        state["alerts_sent"] = state["alerts_sent"] + 1
                    except Exception:
                        pass

            except asyncio.CancelledError:
                raise  # Re-raise to exit the loop
            except Exception as e:
                logger.error(f"Volume monitor error: {e}")

            # Wait for next check
            await asyncio.sleep(config.interval_sec)

    except asyncio.CancelledError:
        # Send stop notification
        elapsed = int(time.time() - state["start_time"])
        mins, secs = divmod(elapsed, 60)

        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🔴 *Volume Monitor Stopped*\n"
                    f"{escape_markdown_v2(config.trading_pair)}\n"
                    f"Duration: {mins}m {secs}s \\| Updates: {state['updates']} \\| Alerts: {state['alerts_sent']}"
                ),
                parse_mode="MarkdownV2"
            )
        except Exception:
            pass

        return f"Stopped after {mins}m {secs}s, {state['updates']} updates, {state['alerts_sent']} alerts"
