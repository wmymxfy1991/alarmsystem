# coding: utf-8
from sqlalchemy import create_engine
from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, JSON, String, Table, Text, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from config.config import *

# 使用方法
# from config.db_models import session, t_order_info
# sc = session()
# orders = sc.query(t_order_info).filter(t_order_info.c.exchange == 'Binance').limit(10)
# for order in orders:
#     print(order)
# sc.close()


dbinfo = {
    'host': CONFIG_GLOBAL['DATABASE_IP'],
    'port': CONFIG_GLOBAL['DATABASE_PORT'],
    'db': CONFIG_GLOBAL['DATABASE_DB'],
    'user': CONFIG_GLOBAL['DATABASE_USER'],
    'pass': CONFIG_GLOBAL['DATABASE_PASS'],
}

db_url = 'postgresql+psycopg2://%(user)s:%(pass)s@%(host)s:%(port)s/%(db)s' % dbinfo

Base = declarative_base()
metadata = Base.metadata


def session():
    Session = sessionmaker(bind=create_engine(
        db_url,
        encoding='utf-8',
        pool_recycle=3600,
        echo=False
    ))
    return Session()


t_order_info = Table(
    'order_info', metadata,
    Column('exchange', String(30), nullable=False),
    Column('order_id', String(100), nullable=False),
    Column('symbol', String(30)),
    Column('contract_type', String(30)),
    Column('strategy_name', String),
    Column('strategy_key', String),
    Column('direction', String),
    Column('price', Float(53)),
    Column('quantity', Float(53)),
    Column('filled_quantity', Float(53)),
    Column('avg_executed_price', Float(53)),
    Column('last_updated_time', String, nullable=False, index=True),
    Column('created_time', String),
    Column('status', String),
    Column('order_type', String),
    Column('account_id', String),
    Column('notes', JSON),
    Index('order_info_sn_s_idx', 'strategy_name', 'status')
)


class PnlInfo(Base):
    __tablename__ = 'pnl_info'

    account = Column(String(30), primary_key=True, nullable=False)
    pnl_snapshot = Column(String)
    last_updated_time = Column(String, primary_key=True, nullable=False)


class PositionInfo(Base):
    __tablename__ = 'position_info'

    account = Column(String(30), primary_key=True, nullable=False)
    data_type = Column(String(100), primary_key=True, nullable=False)
    position_info = Column(String)
    last_updated_time = Column(String, primary_key=True, nullable=False)


class PositionInfoExtend(Base):
    __tablename__ = 'position_info_extend'

    account = Column(String(30), primary_key=True, nullable=False)
    last_updated_time = Column(String, primary_key=True, nullable=False)
    exchange = Column(String(30), primary_key=True, nullable=False)
    accountid = Column(String(30), primary_key=True, nullable=False)
    spot_position = Column(String)
    future_position = Column(String)
    spot_userinfo = Column(String)
    future_userinfo = Column(String)
    spot_balance = Column(String)


class StrategyOrder(Base):
    __tablename__ = 'strategy_order'

    account_id = Column(String, nullable=False)
    order_id = Column(String, primary_key=True, nullable=False)
    exchange = Column(String, primary_key=True, nullable=False)
    symbol = Column(String)
    contract_type = Column(String)
    direction = Column(String)
    quantity = Column(Float(53))
    filled_quantity = Column(Float(53))
    avg_price = Column(Float(53))
    status = Column(String)
    created_time = Column(String)
    last_updated_time = Column(DateTime(True))
    metadata_ = Column('metadata', JSON)


class StrategyPositionInfo(Base):
    __tablename__ = 'strategy_position_info'

    account = Column(String(30), primary_key=True, nullable=False)
    data_type = Column(String(100), primary_key=True, nullable=False)
    position_info = Column(String)
    last_updated_time = Column(String, primary_key=True, nullable=False)


class StrategyPositionInfoExtend(Base):
    __tablename__ = 'strategy_position_info_extend'

    account = Column(String(30), primary_key=True, nullable=False)
    last_updated_time = Column(String, primary_key=True, nullable=False)
    strategy_act = Column(String, primary_key=True, nullable=False)
    strategy_key = Column(String, primary_key=True, nullable=False)
    position = Column(String)
