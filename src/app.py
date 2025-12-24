from flask import Flask, jsonify, send_from_directory, request
import pandas as pd
import psycopg2
from pathlib import Path
from flask_cors import CORS

from pathlib import Path
STATIC_DIR  = Path(__file__).parent / "static"

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path='')
CORS(app)

# connect to the db
def connect_to_db():
    conn = psycopg2.connect(
        host = "localhost",
        database = "financedb",
        user = "findan",
        password = "981232",
        port = 5432
    )
    return conn

@app.route("/api/transactions")
def get_transactions():
    conn = connect_to_db()
    cur = conn.cursor()
    
    id = request.args.get("id")
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")
    type = request.args.get("type")
    source = request.args.get("source")
    category = request.args.get("category")
    currency = request.args.get("currency")
    offset = request.args.get("offset", 50)  

    sql = """SELECT id, date, description, amount, currency, fee, source, type, balance, 
                    balance_pln, balance_usd, balance_eur, balance_kzt,
                    created_at, updated_at 
             FROM transactions"""
    where_parts = []
    params = []

    if currency:
        where_parts.append("currency = %s")
        params.append(currency)

    if category:
        where_parts.append("category = %s")
        params.append(category)

    if id:
        where_parts.append("id = %s")
        params.append(id)

    if type:
        where_parts.append("type = %s")
        params.append(type)

    if source:
        where_parts.append("source = %s")
        params.append(source)

    if date_from:
        where_parts.append("date >= %s")
        params.append(date_from)

    if date_to:
        where_parts.append("date <= %s")
        params.append(date_to)

    if where_parts:
        sql += " WHERE " + " AND ".join(where_parts)

    sql += " ORDER BY date DESC LIMIT %s" 
    params.append(int(offset))
    
    df = pd.read_sql_query(sql, conn, params=params)
    result = df.to_dict(orient="records")
    return jsonify(result)

@app.route("/api/summary/current-month")
def api_current_month_summary():
    currency = request.args.get("currency", "PLN")
    balance_columns = {'PLN': 'balance_pln', 'USD': 'balance_usd', 'EUR': 'balance_eur', 'KZT': 'balance_kzt'}
    
    if currency not in balance_columns:
        return jsonify({"error": "Invalid currency"}), 400
    
    conn = connect_to_db()
    balance_column = balance_columns[currency]

    sql_month = f"""
    SELECT 
        COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS total_income,
        COALESCE(SUM(CASE WHEN amount < 0 THEN amount ELSE 0 END), 0) AS total_expense,
        COALESCE(SUM(amount), 0) AS net_cash_flow
    FROM transactions
    WHERE date >= date_trunc('month', current_date)
        AND date < date_trunc('month', current_date) + interval '1 month'
        AND currency = %s
    """
    
    # Получаем актуальный баланс (последняя транзакция)
    sql_balance = f"""
    SELECT COALESCE({balance_column}, 0) AS current_balance
    FROM transactions
    WHERE {balance_column} IS NOT NULL
    ORDER BY date DESC, id DESC
    LIMIT 1
    """
    
    df_month = pd.read_sql_query(sql_month, conn, params=[currency])
    df_balance = pd.read_sql_query(sql_balance, conn)
    conn.close()
    
    current_balance = float(df_balance['current_balance'][0]) if len(df_balance) > 0 else 0.0
    
    result = {
        "total_income": float(df_month['total_income'][0]),
        "total_expense": float(abs(df_month['total_expense'][0])),
        "net_cash_flow": float(df_month['net_cash_flow'][0]),
        "current_balance": current_balance,
        "currency": currency
    }
    
    return jsonify(result)

@app.route("/api/transactions/latest")
def api_transactions_latest():
    """Получение последних транзакций для выбранной валюты (оригинальные данные)"""
    limit = request.args.get("limit", 10)
    currency = request.args.get("currency", "PLN")
    
    conn = connect_to_db()
    
    # Если выбрана конкретная валюта, показываем только транзакции в этой валюте
    # Если не выбрана или "ALL", показываем все транзакции
    if currency and currency != "ALL":
        sql = """
        SELECT 
            id, date, description, amount, currency, type, source, balance,
            balance_pln, balance_usd, balance_eur, balance_kzt
        FROM transactions
        WHERE currency = %s
        ORDER BY date DESC, id DESC
        LIMIT %s
        """
        params = [currency, int(limit)]
    else:
        sql = """
        SELECT 
            id, date, description, amount, currency, type, source, balance,
            balance_pln, balance_usd, balance_eur, balance_kzt
        FROM transactions
        ORDER BY date DESC, id DESC
        LIMIT %s
        """
        params = [int(limit)]
    
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    
    result = df.to_dict(orient="records")
    return jsonify(result)

@app.route("/api/stats/monthly")
def api_monthly_stats():
    """Статистика по месяцам за последние 12 месяцев для выбранной валюты"""
    currency = request.args.get("currency", "PLN")
    
    balance_columns = {'PLN': 'balance_pln', 'USD': 'balance_usd', 'EUR': 'balance_eur', 'KZT': 'balance_kzt'}
    if currency not in balance_columns:
        return jsonify({"error": "Invalid currency"}), 400
    
    conn = connect_to_db()
    
    sql = f"""
    SELECT 
        TO_CHAR(DATE_TRUNC('month', date), 'YYYY-MM') as month,
        COALESCE(SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END), 0) AS income,
        COALESCE(SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END), 0) AS expense,
        COALESCE(SUM(amount), 0) AS net_flow
    FROM transactions
    WHERE date >= DATE_TRUNC('month', current_date) - INTERVAL '11 months'
        AND currency = %s
    GROUP BY DATE_TRUNC('month', date)
    ORDER BY DATE_TRUNC('month', date)
    """
    
    df = pd.read_sql_query(sql, conn, params=[currency])
    conn.close()
    
    # Конвертируем типы данных
    if len(df) > 0:
        df['income'] = df['income'].astype(float)
        df['expense'] = df['expense'].astype(float)
        df['net_flow'] = df['net_flow'].astype(float)
    
    result = df.to_dict(orient="records")
    return jsonify(result)

@app.route("/api/stats/categories")
def api_categories_stats():
    """Статистика по категориям за текущий месяц для выбранной валюты"""
    currency = request.args.get("currency", "PLN")
    
    balance_columns = {'PLN': 'balance_pln', 'USD': 'balance_usd', 'EUR': 'balance_eur', 'KZT': 'balance_kzt'}
    if currency not in balance_columns:
        return jsonify({"error": "Invalid currency"}), 400
    
    conn = connect_to_db()
    
    # Статистика по оригинальным транзакциям в выбранной валюте
    sql = f"""
    SELECT 
        type as category,
        COALESCE(SUM(ABS(amount)), 0) AS total_amount,
        COUNT(*) as transaction_count
    FROM transactions
    WHERE date >= date_trunc('month', current_date)
        AND date < date_trunc('month', current_date) + interval '1 month'
        AND amount < 0  -- только расходы
        AND currency = %s
    GROUP BY type
    ORDER BY total_amount DESC
    """
    
    df = pd.read_sql_query(sql, conn, params=[currency])
    conn.close()
    
    df['total_amount'] = df['total_amount'].astype(float)
    df['transaction_count'] = df['transaction_count'].astype(int)
    
    result = df.to_dict(orient="records")
    return jsonify(result)

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "dashboard.html") 

if __name__ == "__main__":
    app.run(debug=True)