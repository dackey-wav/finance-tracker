from sqlalchemy import MetaData, Table, Column, String, Date, BigInteger, Numeric
from decimal import Decimal
from datetime import date
from sqlalchemy.orm import Mapped, mapped_column
from db import Base

class TransactionsModel(Base):
    __tablename__ = "transactions" 

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, nullable=False, autoincrement=True)
    transaction_date: Mapped[date] = mapped_column(Date, nullable=False)
    description: Mapped[str] = mapped_column(String, nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 6), nullable=False)
    fee: Mapped[Decimal] = mapped_column(Numeric(14, 6), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    category: Mapped[str | None] = mapped_column(String(50), nullable=True)
    balance: Mapped[Decimal] = mapped_column(Numeric(14, 6), nullable=False)
    source: Mapped[str] = mapped_column(String(50), nullable=False)
    tx_hash: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)