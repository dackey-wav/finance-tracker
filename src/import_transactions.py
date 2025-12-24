import pandas as pd
from pathlib import Path
import hashlib
from decimal import Decimal
from models import TransactionsModel
from db import session_factory, engine
from sqlalchemy.exc import IntegrityError

DATA_DIR  = Path(__file__).parent

def find_all_csv(directory):
    return list(Path(directory).rglob("*.csv"))

def generate_hash(df):
    row_str = f"{df['transaction_date']}{df['amount']}{df['currency']}{df['balance']}"
    hash_value = hashlib.md5(row_str.encode()).hexdigest()
    return hash_value

def read_csv(f):
    required_columns = {
        "transaction_date": ["date", "started date", "дата", "дата начала"],
        "completed_date": ["completed date", "дата завершения"],
        "description": ["description", "описание"],
        "amount": ["amount", "сумма"],
        "currency": ["currency", "валюта"],
        "type": ["type", "тип"],
        "fee": ["fee", "комиссия"],
        "balance": ["balance", "баланс"]
    }

    df = pd.read_csv(f)
    df.columns = [col.lower() for col in df.columns]
    columns = list(df.columns)
    for key in required_columns:
        if any(variant in columns for variant in required_columns[key]):
            print(f"Required column ({required_columns[key]}) - ok")
        else:
            print(f"Required column ({required_columns[key]}) not found in {f.name}")
    
    rename_map = {
        "date": "transaction_date",
        "started date": "transaction_date",
        "дата": "transaction_date",
        "дата начала": "transaction_date",
        "completed date": "completed_date",
        "дата завершения": "completed_date", 
        "description": "description",
        "описание": "description",
        "amount": "amount",
        "сумма": "amount",
        "currency": "currency",
        "валюта": "currency",
        "type": "type",
        "тип": "type",
        "fee": "fee",
        "комиссия": "fee",
        "balance": "balance",
        "баланс": "balance"
        }

    source = f.name.split("_")[0]
    df["source"] = source

    needed  = ["transaction_date", "completed_date", "description", "amount", "fee", "currency", "type", "balance", "source"]
    df = df.rename(columns=rename_map)
    df = df.reindex(columns=needed)

    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors='coerce')
    
    invalid_dates = df[df["transaction_date"].isna()]
    if not invalid_dates.empty:
        print(f"Rows with invalid 'date', will use 'completed_date': {len(invalid_dates)}")
        df.loc[df["transaction_date"].isna(), "transaction_date"] = df.loc[
            df["transaction_date"].isna(), "completed_date"
        ]
    
    still_invalid = df[df["transaction_date"].isna()]
    if not still_invalid.empty:
        print(f"WARNING: {len(still_invalid)} rows still have invalid dates and will be removed")
        df = df[df["transaction_date"].notna()].copy()
    
    df = df.drop(columns=["completed_date"])

    df["amount"] = pd.to_numeric(df["amount"], errors='coerce')
    df["fee"] = pd.to_numeric(df["fee"], errors='coerce')
    df["description"] = df["description"].astype(str)
    df["type"] = df["type"].astype(str)
    df["currency"] = df["currency"].astype(str)
    df["balance"] = pd.to_numeric(df["balance"], errors="coerce")

    allowed_currencies = ["PLN", "EUR", "USD"]
    unexpected_currencies = df.loc[~df["currency"].isin(allowed_currencies), "currency"].unique()
    if len(unexpected_currencies) > 0:
        print(f'Unexpected Currencies: {unexpected_currencies}')

    invalid_amounts = df[df["amount"].isna() | (df["amount"] == 0)]
    print(invalid_amounts)
    
    df["amount"] = df["amount"].fillna(0) 
    df["fee"] = df["fee"].fillna(0)

    df["tx_hash"] = df.apply(generate_hash, axis=1)
    duplicates = df[df.duplicated("tx_hash", keep=False)]
    if not duplicates.empty:
        print("Duplicates found:", duplicates)

    return df

def write_to_db(df):
    with session_factory() as session:
        for row in df.itertuples(index=False):
            try:
                with session.begin_nested(): # SAVEPOINT
                    session.add(TransactionsModel(
                        transaction_date=row.transaction_date,
                        description=row.description,
                        amount=Decimal(str(row.amount)),
                        fee=Decimal(str(row.fee)),
                        currency=row.currency,
                        type=row.type,
                        balance=Decimal(str(row.balance)),
                        source=row.source,
                        tx_hash=row.tx_hash
                    ))
            except IntegrityError as e:
                print(f"Error inserting row with hash {row['hash']}: {e}")
        session.commit()
    print(f"Imported {len(df)} rows")

