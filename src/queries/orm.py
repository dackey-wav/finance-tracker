from sqlalchemy import select
from db import engine, session_factory, Base
from models import TransactionsModel
from datetime import datetime
import hashlib
from decimal import Decimal

def create_tables():
    engine.echo = True
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

def insert_data():
    transaction_date = datetime.now()
    description = 'Żabka'
    amount = Decimal('150.65')
    fee = Decimal('0.00')
    currency = 'PLN'
    type = 'Card Payment'
    balance = Decimal('1000.00')
    source = 'Revolut'
    tx_hash = hashlib.md5(f'{str(transaction_date)}{str(amount)}{currency}{str(balance)}'.encode()).hexdigest()

    with session_factory() as session:
        tran_obj = TransactionsModel(
                        transaction_date=transaction_date,
                        description=description,
                        amount=amount,
                        fee=fee,
                        currency=currency,
                        type=type,
                        balance=balance,
                        source=source,
                        tx_hash=tx_hash
                            )
        session.add(tran_obj)
        session.flush()
        session.commit()

    print("Test transaction created")
    session.close()

def get_transactions():
    with session_factory() as session:
        query = select(TransactionsModel)
        result = session.execute(query)
        transactions = result.scalars().all 
        print(f"{transactions=}")