from decimal import Decimal

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from hummingbot.connector.connector_base import TradeType
from plotly.subplots import make_subplots

from data_structures.candles import Candles
from visualization.theme import get_default_layout


def add_executors_trace(fig, executors, row, col):
    for executor in executors:
        entry_time = pd.to_datetime(executor.timestamp, unit='s')
        entry_price = executor.custom_info["current_position_average_price"]
        exit_time = pd.to_datetime(executor.close_timestamp, unit='s')
        exit_price = executor.custom_info["close_price"]
        name = "Buy Executor" if executor.config.side == TradeType.BUY else "Sell Executor"

        if executor.filled_amount_quote == 0:
            fig.add_trace(go.Scatter(x=[entry_time, exit_time], y=[entry_price, entry_price], mode='lines',
                                     line=dict(color='grey', width=2, dash="dash"), name=name), row=row, col=col)
        else:
            if executor.net_pnl_quote > Decimal(0):
                fig.add_trace(go.Scatter(x=[entry_time, exit_time], y=[entry_price, exit_price], mode='lines',
                                         line=dict(color='green', width=3), name=name), row=row, col=col)
            else:
                fig.add_trace(go.Scatter(x=[entry_time, exit_time], y=[entry_price, exit_price], mode='lines',
                                         line=dict(color='red', width=3), name=name), row=row, col=col)
    return fig


def get_pnl_trace(executors):
    pnl = [e.net_pnl_quote for e in executors]
    cum_pnl = np.cumsum(pnl)
    return go.Scatter(
        x=pd.to_datetime([e.close_timestamp for e in executors], unit="s"),
        y=cum_pnl,
        mode='lines',
        line=dict(color='gold', width=2, dash="dash"),
        name='Cumulative PNL'
    )


def create_backtesting_figure(df, executors, config):
    # Create subplots
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        vertical_spacing=0.02, subplot_titles=('Candlestick', 'PNL Quote'),
                        row_heights=[0.7, 0.3])

    candles = Candles(df, config.get("connector_name"), config.get("trading_pair"))

    # Add candlestick trace
    fig.add_trace(candles.candles_trace(), row=1, col=1)

    # Add executors trace
    fig = add_executors_trace(fig, executors, row=1, col=1)

    # Add PNL trace
    fig.add_trace(get_pnl_trace(executors), row=2, col=1)

    # Apply the theme layout
    layout_settings = get_default_layout(f"Trading Pair: {config['trading_pair']}")
    layout_settings["showlegend"] = False
    fig.update_layout(**layout_settings)

    # Update axis properties
    fig.update_xaxes(rangeslider_visible=False, row=1, col=1)
    fig.update_xaxes(row=2, col=1)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="PNL", row=2, col=1)
    return fig
