# encoding: utf-8
from enum import Enum


class Algorithms(Enum):
    TWAP = 'TWAP'
    ICEBERG = 'ICEBERG'
    TRIANGLE_TWAP = 'T-TWAP'
    TRIANGLE_ICEBERG = 'T-ICEBERG'
    SAMPLE = 'SAMPLE'
    VWAP = 'VWAP'


class AlarmCode(Enum):
    DATA_OUTDATED = "050003"
    DATA_UNRECEIVED = "050004"
    EXECUTE_ABNORMAL = "050006"
    ORDER_RESPONSE_EXCEPTION = '050005'
    DEAL_SIZE_NOT_UPDATED = '080001'


class CurrencyType(Enum):
    QUOTE = "Quote"
    BASE = "Base"


class PublishChannel(Enum):
    UI = "UI"
    PDT = "PDT"
    ALARM = "ALARM"


class Event(Enum):
    ON_LOG = "create_new_log"
    ON_ERROR = "error_occurred"
    ON_MARKET_DATA_ORIGIN = "market_data_updated_from_exchange"
    ON_MARKET_DATA_DB = "market_data_updated_after_filter_to_db"
    ON_MARKET_DATA = "market_data_updated_after_filter"
    ON_ORDER = "order_updated_from_exchange"
    ON_TRADE = "trade_updated_from_exchange"
    ON_POSITION = "position_updated_from_exchange"
    ON_MARKET_DATA_READY = "market_data_ready_for_strategy"
    ON_ORDER_READY = "order_ready_for_strategy"
    ON_TRADE_READY = "trade_ready_for_strategy"
    ON_POSITION_READY = "position_ready_for_strategy"
    ON_DISCONNECT = "disconnected"
    ON_CONNECT = "connected"
    ON_RECONNECT = "reconnected"
    ON_FAIL_CANCEL_ORDER = "fail_to_cancel_order"
    ON_FAIL_INSPECT_ORDER = "fail_to_inspect_order"
    ON_FEED_MARKET_DATA = "market_data_feeded_to_downstream"
    ON_RESET_ORDERBOOK = "reset_orderbook"
    ON_ORDER_RECORD = "save_order_into_db"
    ON_STRATEGY_POSITION_RECORD = "strategy_position_update_ready"
    ON_PNL_SNAPSHOT = "pnl_snapshot_into_db"
    ON_STRATEGY_ORDER = "fetch_strategy_order_from_ex_to_db"
    ON_STRATEGY_ENABLEMENT_UPDATE = "strategy_enablement_update"
    ON_EXCHANGE_ENABLEMENT_UPDATE = "exchange_enablement_update"
    ON_LIVE_TRADING_ENABLEMENT_UPDATE = "live_trading_enablement_update"
    ON_AUTO_BALANCE_ORDER = "order_to_balance_risk"
    ON_SEND_ORDER = "send_order"
    ON_STRATEGY_ALARM = "strategy_alarm"
    ON_RESET_STRATEGY_MONITOR = "reset_strategy_monitor"
    ON_STRATEGY_UPDATE = "strategy_update"
    ON_UI_UPDATE = "update_strategy_status"
    ON_CREDENTIALS_UPDATE = "credentials_update"
    ON_SET_ENABLEMENT = "set_enablement"
    ON_STOP_STRATEGY = "stop_strategy"
    ON_UPDATE_STRATEGY = "update_strategy"
    ON_SYNC_POSITION = "sync_strategy_position"
    ON_ENABLEMENT_changed = "enablement_changed"
    ON_FEED_MANAGER_START = "feed_manager_starts"
    ON_POSITION_MANAGER_START = "position_manager_starts"
    ON_ORDER_MANAGER_START = "order_manager_starts"


class Aggregation(Enum):
    TICK = "tick"
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"


class TradingStatus(Enum):
    START = 1
    STOP = 0
    # LIMIT     =     0


class MarketDataType(Enum):
    """
    market data type
    """
    QUOTE = "quote"
    ORDERBOOK = "orderbook"
    TRADE = "trade"
    INDEX = "index"
    FUNDING = "funding"
    HOLDAMOUNT = "holdamount"
    KLINE = "kline"
    PRICE = "price"
    RATE = "rate"
    QUOTETICKER = "quote_ticker"
    LIQUIDATION = "liquidation"
    QUOTESTREAM = "quotestream"


# orderbook update type=
# SNAPSHOT     = directly update orderbook by its snapshots
# QUOTESTREAM  = initialize orderbook by a snapshot
class OrderbookUpdateType(Enum):
    SNAPSHOT = "snapshot"
    QUOTE_STREAM = "quote_stream"


class SpreadTradeExecutionAction(Enum):
    ENTER = "enter"
    BALANCE = "balance"


class OrderType(Enum):
    LIMIT = "limit"
    MARKET = "market"
    FAK = "fak"
    FOK = "fok"
    FUT = "fut"
    STOP = "stop"
    POST_ONLY = "post-only"
    STOP_LIMIT = "stop_limit"


class TaskStatus(Enum):
    PENGING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    WARNING = "warning"
    ERROR = "error"
    DELETED = "deleted"
    FINISHED = "finished"


class Command(Enum):
    START = 'start'
    PAUSE = 'pause'
    RESUME = 'resume'
    DELETE = 'delete'
    DOWNLOAD = 'download'
    # Task订单相关
    STATISTICS = 'statistics'
    EXPORT_STATISTICS = 'export_statistics'
    OMS_SEND_ORDER = 'oms_send_order'
    OMS_CANCEL_ORDER = 'oms_cancel_order'
    OMS_INSPECT_ORDER = 'oms_inspect_order'
    OMS_CANCEL_ALL_ORDER = 'oms_cancel_all_order'
    OMS_ORDER_STATUS = 'oms_order_status'
    OMS_FINISHED_ORDERS = 'oms_finished_orders'
    OMS_UNFINISHED_ORDERS = 'oms_unfinished_orders'


class MasterCommand(Enum):
    GET_BALANCE = 'get_balance'
    DOWNLOAD = 'download'
    INSPECT = 'inspect'


class FeedSubscriptionAction(Enum):
    SUB = "sub"
    UNSUB = "unsub"


class OrderStatus(Enum):
    PENDING = "pending"
    SUBMITTED = "new"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    FILLED = "filled"
    UNTRIGGERED = "untriggered"  # only for stop_limit and stop_market order
    PARTIALLY_FILLED = "partially_filled"


class WithdrawStatus(Enum):
    SUBMITTED = "new"
    CANCELLED = "cancelled"
    SUCCEED = "succeed"
    FAILED = "failed"


class DepositStatus(Enum):
    SUBMITTED = "new"
    CANCELLED = "cancelled"
    SUCCEED = "succeed"
    FAILED = "failed"


class TradeSide(Enum):
    BUY = "buy"
    SELL = "sell"


class Direction(Enum):
    BUY = "Buy"
    SELL = "Sell"
    COVER = "Cover"
    SHORT = "Short"


class RequestActions(Enum):
    SEND_ORDER = "place_order"
    CANCEL_ORDER = "cancel_order"
    INSPECT_ORDER = "inspect_order"
    INSPECT_ORDER_BATCH = "inspect_order_batch"
    CANCEL_ALL_ORDER = "cancel_all_orders"
    QUERY_POSITION = "query_position"
    QUERY_BALANCE = "query_balance"
    QUERY_SUBACCOUNT_BALANCE = "query_subaccount_balance"
    QUERY_ORDERS = "query_orders"
    QUERY_MARGIN = "query_margin"
    DEPOSIT = "deposit"
    WITHDRAW = "withdraw"
    QUERY_HISTORY_ORDERS = "query_history_orders"
    QUERY_HOLD_AMOUNT = "query_hold_amount"
    QUERY_LATEST_PRICE = "query_latest_price"
    QUERY_LIQUIDATION = "query_liquidation"
    QUERY_HISTORY_TRADES = "query_history_trades"
    QUERY_WITHDRAWALS = "query_withdrawals_history"
    QUERY_DEPOSITS = "query_deposits_history"
    GET_WALLET_ADDRESS = "get_wallet_address"
    UPDATE_COMMENTS = "update_comments"
    MODIFY_ORDER = "modify_order"
    ADD_RECORD = "add_record"
    LOAN = "loan"
    TRANS_ASSET = "trans_asset"
    REPAY = "repay"
    QUERY_LOAN_HISTORY = "query_loan_history"
    TRANSFER = "transfer"
    SEND_ORDER_BATCH = "place_order_batch"
    GET_QUOTE = "get_quote"


class TransAccountType(Enum):
    CHILD_ACCOUNT = "child_account"
    SPOT_ACCOUNT = "spot_account"
    FUTURE_ACCOUNT = "future_account"
    C2C_ACCOUNT = "c2c_account"
    SPOT_MARGIN_ACCOUNT = "spot_margin_account"
    WALLET_ACCOUNT = "wallet_account"
    ETT_ACCOUNT = "ett_account"
    PERP_ACCOUNT = "perp_account"


class OrderActions(Enum):
    SEND = "place_order"
    CANCEL = "cancel_order"
    INSPECT = "inspect_order"
    CANCEL_ALL = "cancel_all_orders"


class StrategyActions(Enum):
    GET_CONFIG = "get_strategy_config"
    UPDATE_CONFIG = "update_strategy_config"
    GET_MODE_STATUS = "get_mode_status"
    START_MODE = "start_mode"
    STOP_MODE = "stop_mode"
    RESTART_STRATEGY = "restart_strategy"


class Exchange(Enum):
    NAMEBASE = "Namebase"
    OLDOK = "OldOK"
    BITFINEX = "Bitfinex"
    HUOBI = "Huobi"
    BITTREX = "Bittrex"
    GEMINI = "Gemini"
    BINANCE = "Binance"
    BITMEX = "BitMEX"
    BITSTAMP = "Bitstamp"
    HITBTC = "Hitbtc"
    BITFLYER = "Bitflyer"
    IMTOKEN = "Imtoken"
    ZB = "ZBcoin"
    BIBOX = "Bibox"
    FCOIN = "Fcoin"
    COINEGG = "Coinegg"
    COINTIGER = "Cointiger"
    HADAX = "Hadax"
    KUCOIN = "Kucoin"
    QUOINEX = "Quoinex"
    GATEIO = "Gateio"
    COINPARK = "Coinpark"
    BITHUMB = "Bithumb"
    BITHUMB_NORMAL = "Bithumb_normal"
    DEXTOP = "Dextop"
    TRADEIO = "Tradeio"
    BTCC = "Btcc"
    COINEX = "Coinex"
    IDEX = "Idex"
    COINSUPER = "Coinsuper"
    BITFOREX = "Bitforex"
    BGOGO = "Bgogo"
    KRYPTONO = "Kryptono"
    UPBIT = "Upbit"
    DERIBIT = "Deribit"
    DERIBIT_V2 = "Deribit_V2"
    GAEA = "Gaea"
    CPDAX = "Cpdax"
    EXMO = "Exmo"
    BILAXY = "Bilaxy"
    BITSO = "Bitso"
    BITSONIC = "Bitsonic"
    ALAMEDA = "Alameda"
    NEWOKCOIN = "NewOKcoin"
    OKCOIN = "OKcoin"
    HUOBIKR = "HuobiKr"
    BCOIN = "Bcoin"
    QUADRIGACX = "Quadrigacx"
    LUNO = "Luno"
    BITMAX = "Bitmax"
    KRAKEN = "Kraken"
    COINONE = "Coinone"
    BW = "Bw"
    GOPAX = "Gopax"
    CEX = "Cex"
    POLONIEX = "Poloniex"
    IB = "Ib"
    HUOBIFUTURE = "HuobiFuture"
    BTCMARKET = "Btcmarket"
    BYBIT = "Bybit"
    NEGOCIECOINS = "NegocieCoins"
    INDODAX = "Indodax"
    BXINTH = "Bxinth"
    IG = "Ig"
    BITMART = "Bitmart"
    MERCADOBITCOIN = "MercadoBitcoin"
    BEQUANT = "Bequant"
    BITASSET = "Bitasset"
    BITBAY = "Bitbay"
    DRAGONEX = "Dragonex"
    DSXUK = "Dsxuk"
    COINALL = "Coinall"
    COINROOM = "Coinroom"
    COINBASE = "Coinbase"
    GOEXCHANGE = "Goexchange"
    NEWKUCOIN = "NewKucoin"
    IDAX = "Idax"
    ALLBIT = "Allbit"
    PANTOSHI = "Pantoshi"
    PANTOSHIBETA = "PantoshiBeta"
    COINFLEX = "Coinflex"
    CRYPTOFACILITIES = "Cryptofacilities"
    CGEX = "Cgex"
    BTCTURK = "Btcturk"
    KORBIT = "Korbit"
    YAHOO = "Yahoo"
    BBX = "Bbx"
    COINFINIT = "Coinfinit"
    EXX = "Exx"
    FTX = "Ftx"
    FTXOTC = "Ftxotc"
    GDAC = "Gdac"
    COINEAL = "Coineal"
    MXC = "Mxc"
    B2C2 = "B2c2"


class ContractType(Enum):
    SPOT = "spot"
    FUTURE_THIS_WEEK = "this_week"
    FUTURE_NEXT_WEEK = "next_week"
    FUTURE_THIS_MONTH = "this_month"
    FUTURE_THIS_MONTH_FORWARD = "this_month_forward"
    FUTURE_THIS_MONTH_REVERSE = "this_month_reverse"
    FUTURE_THIS_QUARTER = "quarter"
    FUTURE_THIS_QUARTER_FORWARD = "quarter_forward"
    FUTURE_THIS_QUARTER_REVERSE = "quarter_reverse"
    FUTURE_NEXT_QUARTER = "next_quarter"
    FUTURE_NEXT_TWO_QUARTER = "next_two_quarter"
    FUTURE_PERP = "perp"
    FUTURE_PERP_FORWARD = "perp_forward"
    FUTURE_PERP_REVERSE = "perp_reverse"
    FUTURE_LTFX = "lightening_fx"
    FUTURE_INDEX = "index"
    OPTION = "option"
    FOREX = "forex"
    FUTURE_NEXT_MONTH = "next_month"


class PositionInfoType(Enum):
    SPOT_POSITION = "spot_position"
    FUTURE_POSITION = "future_position"
    SPOT_USERINFO = "spot_userinfo"
    FUTURE_USERINFO = "future_userinfo"
    SPOT_BALANCE = "spot_balance"
    OTC_BALANCE = "otc_balance"
    SPOT_SUBACCOUNT_BALANCE = "spot_subaccount_balance"
    OPTION_POSITION = "option_position"
    PHYSICAL_SETTLED_FUTURE_POSITION = "physical_settled_future_position"


class TradeType(Enum):
    SPOT = "spot"
    FUTURE = "future"


class FutureOrderType(Enum):
    LONG = "buy"
    SHORT = "sell"


class FutureSizeIndex(Enum):
    CONTRACT = 0
    COIN = 1
    EXPOSURE_IN_QUOTE_CCY = 2


class AccountType(Enum):
    EXCHANGE = "exchange"
    MARGIN = "margin"


class MarginType(Enum):
    ACCOUNT = "margin_per_account"
    SYMBOL = "margin_per_symbol"
    COMBINED = "margin_combined"


class FutureAccountType(Enum):
    MARGIN_BY_ACCOUNT = "margin_by_account"
    MARGIN_BY_CONTRACT = "margin_by_contract"


class Encryption(Enum):
    RSA = "rsa"
    AES = "aes"


class IntercomChannel(Enum):
    SUBSCRIBE_REQUEST = "Subscribe Request"
    SUBSCRIBE_RESPONSE = "Subscribe Response"
    UNSUBSCRIBE_REQUEST = "Unsubscribe Request"
    UNSUBSCRIBE_RESPONSE = "Unsubscribe Response"
    SEND_ORDER_REQUEST = "Send Order Request"
    SEND_ORDER_RESPONSE = "Send Order Response"
    CANCEL_ORDER_REQUEST = "Cancel Order Request"
    CANCEL_ORDER_RESPONSE = "Cancel Order Response"
    INSPECT_ORDER_REQUEST = "Inspect Order Request"
    INSPECT_ORDER_RESPONSE = "Inspect Order Response"
    INQUIRY_BALANCE_REQUEST = "Inquiry Balance Request"
    INQUIRY_BALANCE_RESPONSE = "Inquiry Balance Response"
    POLL_POSITION_INFO_REQUEST = "Poll Position Request"
    ORDER_UPDATE_SUBSCRIPTION_REQUEST = "Subscribe Order Update"
    STRATEGY_CONFIGURATION = "strategy_configuration"
    STRATEGY_CONFIGURATION_RESPONSE = "strategy_configuration_response"
    UI_HISTROGY_INFO_REQUEST = "history_info_request"
    UI_HISTROGY_INFO_RESPONSE = "history_info_response"
    STRATEGY_CONFIG_UPDATE = "strategy_config_update"
    STRATEGY_CONFIG_UPDATE_RESPONSE = "strategy_config_update_response"
    SERVER_STATUS = "server_status"


class IntercomScope(Enum):
    MARKET = "Md_beta05"  # "Md_beta05" test is Md_beta05_debug
    TRADE = "Td_beta05"
    POSITION = "Position_beta05"
    MARKETREQ = "Md0"
    RISK = "Risk"
    UI = "UI"
    ALARM = "MM"
    CONSOLE = "Cnsl_beta05"
    STRATEGY = "Sc"
    ED = "ED"


class IntercomScope8(Enum):
    MARKET = "Md_beta08"
    TRADE = "Td_beta08"
    POSITION = "Position_beta08"
    MARKETREQ = "Md0"
    RISK = "Risk"
    UI = "UI"
    ALARM = "MM"
    CONSOLE = "Cnsl_beta08"
    STRATEGY = "Sc"
    ED = "ED"
