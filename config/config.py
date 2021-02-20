# encoding: utf-8
import os
from raven import Client

CONFIG_PATH = os.path.dirname(os.path.abspath(__file__))
ROOT_PATH = os.path.dirname(CONFIG_PATH)

# 使用阿里云内网地址通信
sentry = Client('http://39b1c2f2f89d40d7bad906157a1c5bef@172.31.228.60:9000/2')
# sentry = Client('http://39b1c2f2f89d40d7bad906157a1c5bef@47.52.140.130:9000/2')

# sentry 路径47.52.140.130:/root/onpremise
# 启动命令 docker-compose down && docker-compose up -d

CONFIG_GLOBAL = {
    "TIME_INTERVAL": 3,  # 定时任务的时间间隔

    "REDIS_ADD_TASK_QUEUE": "eaas_add_task",  # 任务队列
    "REDIS_TASK_STATUS": "eaas_task_status",  # 定时向外部推送task的状态信息
    "REDIS_TASK_COMMAND": "eaas_task_command",  # 控制task的暂停, 恢复, 删除
    "REDIS_TASK_COMMAND_RESP": "eaas_task_command_response",  # 控制命令返回信息
    "REDIS_NOTIFICATION": "eaas_notification",  # UI 通知通道
    "REDIS_STATUS_MONITOR": "eaas_status_monitor",  # hash表, 存着所有task最新的status

    "REDIS_MASTER_COMMAND": "eaas_master_command",  # 控制master执行任务
    "REDIS_MASTER_COMMAND_RESP": "eaas_master_command_response",  # master控制命令返回信息

    "STRATEGY_NAME": "eaas_execution",

    "TASK_HANDLER": "driver.py",
    "BALANCE_HANDLER": "balance.py",
    "ORDER_HANDLER": "order_control.py",
    "MASTER_HANDLER": "master.py",

    # "REDIS_UI": ["172.31.228.82", 55556, "YjFfcxyfUfwk1CZf", 0],  # eaas1.0 test
    # "REDIS_UI": ["172.31.228.82", 55554, "k2iENg2cyjzP#s8y", 0],  # eaas2.0 test
    # "REDIS_UI": ["172.24.247.97", 55556, "EHvcbYUF^wh!CSlG", 0],  # eaas2.0 test

    "REDIS_UI": ["172.31.228.170", 55556, "5H9Lx#q$aLh45fiO", 0],  # eaas2.0 prod

    # "REDIS_UI": ["172.31.228.79", 6379, "pass1", 0],  # eaas1.0 prod
    "REDIS_PDT": ["172.31.227.228", 6379, "pass1", 0],
    "REDIS_ALARM": ["172.31.227.199", 6379, "pass1", 0],

    "DATABASE_IP": "172.31.227.199",
    "DATABASE_PORT": 5432,
    "DATABASE_USER": "user1",
    "DATABASE_DB": "pdt",
    "DATABASE_PASS": "pass1",
}

# sqlacodegen postgresql+psycopg2://user1:pass1@172.31.227.199:5432/pdt --outfile dbmodel.py


# max order size by quote
MAX_SIZE_BY_QUOTE = {
    "USD": 2000,
    "USDT": 2000,
    "TUSD": 2000,
    "PAX": 2000,
    "USDC": 2000,
    "HT": 800,
    "BNB": 100,
    "ETH": 10,
    "BTC": 0.2,
    "KRW": 2000000
}
# supportd exchanges of on_order_update in pdt
ORDER_UPDATE_EX = {
    'Binance': True,
    'Ftx': False,
    'Bitfinex': True,
    'OKcoin': True,
    'Coinflex': False,
}
BALANCE_BY_ORDER_RES_EX = {
    'Binance': True,
    'Huobi': True,
    'Bitfinex': True,
    'OKcoin': True,
    'NewKucoin': True
}

DEAL_SIZE_MAX_DISPLAY = 10
TIMEOUT = 3
EXCHANGE_KLINE_AVAILABLE = {
    "Binance": {
        "URL": "https://www.binance.com/api/v1/klines?symbol={}&interval=1h",
    },
    "Huobi": {
        "URL": "https://api.huobi.pro/market/history/kline?period=60min&size=200&symbol={}",
    },
    "Coinone": {
        "URL": "https://tb.coinone.co.kr/api/v1/chart/olhc/?site=coinone{}&type=1h",
        "SYMBOLS": {
            "LUNAKRW": "luna",
            "BTCKRW": "btc",
        }
    },
    "Gateio": {
        "URL": "https://data.gateio.co/api2/1/candlestick2/{}?group_sec=3600&range_hour=480",
        "SYMBOLS": {
            "DXUSDT": "dx_usdt",
            "VIDYUSDT": "vidy_usdt",
            "BTCUSDT": "btc_usdt",
            "BKCUSDT": "bkc_usdt",
            "ARPAUSDT": "arpa_usdt",
        }
    },
    "Cointiger": {
        "URL": "https://api.cointiger.com/exchange/trading/api/market/history/kline?symbol={}&period=60min&size=200",
        "SYMBOLS": {
            "BTCUSDT": "btcusdt",
            "ETHUSDT": "ethusdt",
            "KINUSDT": "kinusdt"
        }
    },
    "Upbit": {
        "URL": "https://crix-api-cdn.upbit.com/v1/crix/candles/minutes/60?code=CRIX.UPBIT.{}&count=400",
        "SYMBOLS": {
            "UPPKRW": "KRW-UPP",
            "LUNABTC": "BTC-LUNA",
            "BTCKRW": "KRW-BTC",
            "CREKRW": "KRW-CRE"
        }
    },
    "Bitfinex": {
        "URL": "https://api.bitfinex.com/v2/candles/trade:1h:{}/hist?limit=1000",
        "SYMBOLS": {
            "LEOBTC": "tLEOBTC",
            "LEOUSDT": "tLEOUST",
            "BTCUSDT": "tBTCUST",
            'ALGOBTC': "tALGBTC",
            "ALGOUSD": "tALGUSD",
            "ALGOUSDT": "tALGUST"

        }
    },
    "Bitmax": {
        "URL": "https://bitmax.io/api/r/v1/barhist?symbol={}&interval=60&from={}&to={}",
        "SYMBOLS": {
            "BTCUSDT": "BTC-USDT",
            "ALGOUSDT": "ALGO-USDT",
            "ALGOBTC": "ALGO-BTC",
        }
    },
    "Bittrex": {
        "URL": "https://bittrex.com/Api/v2.0/pub/market/GetTicks?marketName={}&tickInterval=thirtyMin",
        "SYMBOLS": {
            "LUNABTC": "BTC-LUNA"
        }
    },
    "Coinbase": {
        "URL": "https://api.pro.coinbase.com/products/{}/candles/1h",
        "SYMBOLS": {
            "ALGOUSD": "ALGO-USD"
        }
    },
    "Okex": {
        "URL": "https://www.okex.com/v2/spot/instruments/{}/candles?granularity=3600&size=1000",
        "SYMBOLS": {
            'ALGOUSDT': "ALGO-USDT",
            'ALGOBTC': "ALGO-BTC",
            'ALGOETH': "ALGO-ETH",
            'ALGOUSDK': "ALGO-USDK",
        }
    },
}

VWAP_SUPPORT_EX = [
    'Binance',
    'Huobi',
    'Bitfinex',
    'Bittrex'
]
EMAIL_INFO = {
    'mail_host': "hwsmtp.exmail.qq.com",
    'mail_user': "trade_reports@amberaigroup.com",
    'mail_pass': "AmberAI123.",

    'sender': 'trade_reports@amberaigroup.com',
    'receivers': {
        "group": [
            "min.tong@amberaigroup.com",
        ],
        "test": [
            "min.tong@amberaigroup.com",
            "daniel.gao@amberaigroup.com",
            "zoran.liu@amberaigroup.com",
            'otc.operations@amberaigroup.com',
            'trade_reports@amberaigroup.com',
            'otc@amberaigroup.com',
        ]
    },
}

DIR_PATH = os.path.join(ROOT_PATH, 'reports')
if not os.path.exists(DIR_PATH):
    os.mkdir(DIR_PATH)

EAAS_SYMBOL_MAP = {
    'ETHUSDT': 'ETHUSD',
    'BTCUSDT': 'BTCUSD',
    'ALGOUSDT': 'ALGOUSD',
    'BEAMUSDT': 'BEAMUSD',
    'USDCUSDT': 'USDCUSD',
    'PAXUSDT': 'PAXUSD',
    'FETUSDT': 'FETUSD'
}

SYMBOL_BASE_QUOTE = {
    'ETHUSD': ['ETH', 'USD'],
    'BTCUSD': ['BTC', 'USD'],
    'ETHUSDT': ['ETH', 'USDT'],
    'BTCUSDT': ['BTC', 'USDT'],
    'DOCKBTC': ['DOCK', 'BTC']
}