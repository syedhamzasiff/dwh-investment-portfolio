from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from io import StringIO
from airflow.sdk import Variable

# Default args
default_args = {
    'owner': 'hamza',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'depends_on_past': False,
}

def get_db_connection(db_name):
    return psycopg2.connect(
        host=Variable.get("EC2_PUBLIC_IP"),
        dbname=db_name,
        user='airflow_user',
        password='airflow_pass',
        port='5432'
    )

def create_dwh_schema():
    stmts = [
        "CREATE SCHEMA IF NOT EXISTS dwh;",
        # Dimensions
        """
        CREATE TABLE IF NOT EXISTS dwh.DimDate (
          date_key INTEGER PRIMARY KEY,
          date DATE NOT NULL,
          day INTEGER NOT NULL,
          day_of_week INTEGER NOT NULL,
          day_name VARCHAR(10) NOT NULL,
          month INTEGER NOT NULL,
          month_name VARCHAR(10) NOT NULL,
          quarter INTEGER NOT NULL,
          year INTEGER NOT NULL,
          is_weekend BOOLEAN NOT NULL,
          is_holiday BOOLEAN NOT NULL
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dwh.DimPortfolio (
          portfolio_key SERIAL PRIMARY KEY,
          portfolio_id VARCHAR(20) NOT NULL UNIQUE,
          portfolio_name VARCHAR(100),
          portfolio_type VARCHAR(50),
          management_style VARCHAR(50),
          risk_tolerance VARCHAR(20),
          target_return NUMERIC(10,4),
          inception_date_key INTEGER,
          last_rebalance_date_key INTEGER,
          benchmark_id VARCHAR(20),
          status VARCHAR(20)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dwh.DimCustomer (
          customer_key SERIAL PRIMARY KEY,
          customer_id VARCHAR(20) NOT NULL UNIQUE,
          full_name VARCHAR(101),
          email VARCHAR(100),
          city VARCHAR(50),
          state VARCHAR(50),
          country VARCHAR(50),
          segment_name VARCHAR(50),
          annual_income NUMERIC(12,2),
          investment_goal VARCHAR(100),
          investment_horizon VARCHAR(50),
          risk_profile_id INTEGER,
          registration_date_key INTEGER,
          is_active BOOLEAN
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dwh.DimAdvisor (
          advisor_key SERIAL PRIMARY KEY,
          advisor_id VARCHAR(20) NOT NULL UNIQUE,
          full_name VARCHAR(101),
          title VARCHAR(100),
          branch_id NUMERIC,
          department VARCHAR(50),
          years_experience NUMERIC,
          certification VARCHAR(255),
          team_id NUMERIC,
          team_name VARCHAR(100),
          status VARCHAR(20)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dwh.DimInstrumentHolding (
          instrument_holding_key SERIAL PRIMARY KEY,
          portfolio_id VARCHAR(20) NOT NULL,
          instrument_id VARCHAR(20) NOT NULL,
          instrument_name VARCHAR(100),
          ticker VARCHAR(20),
          instrument_type VARCHAR(50),
          sector_name VARCHAR(100),
          industry_name VARCHAR(100),
          weight NUMERIC(6,4),
          purchase_date_key INTEGER,
          UNIQUE(portfolio_id, instrument_id)
        );
        """,
        # Fact
        """
        CREATE TABLE IF NOT EXISTS dwh.FactPortfolioPerformance (
          portfolio_performance_key SERIAL PRIMARY KEY,
          date_key INTEGER NOT NULL,
          portfolio_key INTEGER NOT NULL,
          customer_key INTEGER NOT NULL,
          advisor_key INTEGER NOT NULL,
          market_value NUMERIC(15,2),
          deposits NUMERIC(15,2),
          withdrawals NUMERIC(15,2),
          daily_return_percentage NUMERIC(10,6),
          cumulative_return NUMERIC(15,6),
          benchmark_return NUMERIC(10,6),
          alpha NUMERIC(10,6),
          beta NUMERIC(10,6),
          sharpe_ratio NUMERIC(10,6),
          volatility NUMERIC(10,6),
          FOREIGN KEY(date_key)      REFERENCES dwh.DimDate(date_key),
          FOREIGN KEY(portfolio_key) REFERENCES dwh.DimPortfolio(portfolio_key),
          FOREIGN KEY(customer_key)  REFERENCES dwh.DimCustomer(customer_key),
          FOREIGN KEY(advisor_key)   REFERENCES dwh.DimAdvisor(advisor_key),
          UNIQUE(date_key, portfolio_key)
        );
        """,
        # Staging tables
        """
        CREATE TABLE IF NOT EXISTS dwh.stg_dimportfolio (
          portfolio_id VARCHAR(20),
          portfolio_name VARCHAR(100),
          portfolio_type VARCHAR(50),
          management_style VARCHAR(50),
          risk_tolerance VARCHAR(20),
          target_return NUMERIC(10,4),
          inception_date_key INTEGER,
          last_rebalance_date_key INTEGER,
          benchmark_id VARCHAR(20),
          status VARCHAR(20)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dwh.stg_dimcustomer (
          customer_id VARCHAR(20),
          full_name VARCHAR(101),
          email VARCHAR(100),
          city VARCHAR(50),
          state VARCHAR(50),
          country VARCHAR(50),
          segment_name VARCHAR(50),
          annual_income NUMERIC(12,2),
          investment_goal VARCHAR(100),
          investment_horizon VARCHAR(50),
          risk_profile_id INTEGER,
          registration_date_key INTEGER,
          is_active BOOLEAN
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dwh.stg_dimadvisor (
          advisor_id VARCHAR(20),
          full_name VARCHAR(101),
          title VARCHAR(100),
          branch_id NUMERIC,
          department VARCHAR(50),
          years_experience NUMERIC,
          certification VARCHAR(255),
          team_id NUMERIC,
          team_name VARCHAR(100),
          status VARCHAR(20)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dwh.stg_diminstrumentholding (
          portfolio_id VARCHAR(20),
          instrument_id VARCHAR(20),
          instrument_name VARCHAR(100),
          ticker VARCHAR(20),
          instrument_type VARCHAR(50),
          sector_name VARCHAR(100),
          industry_name VARCHAR(100),
          weight NUMERIC(6,4),
          purchase_date_key INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dwh.stg_factperformance (
          date_key INTEGER,
          portfolio_id VARCHAR(20),
          customer_id VARCHAR(20),
          advisor_id VARCHAR(20),
          market_value NUMERIC(15,2),
          deposits NUMERIC(15,2),
          withdrawals NUMERIC(15,2),
          daily_return_percentage NUMERIC(10,6),
          cumulative_return NUMERIC(15,6),
          benchmark_return NUMERIC(10,6),
          alpha NUMERIC(10,6),
          beta NUMERIC(10,6),
          sharpe_ratio NUMERIC(10,6),
          volatility NUMERIC(10,6)
        );
        """
    ]
    conn = get_db_connection('finance_dwh')
    cur = conn.cursor()
    try:
        for s in stmts:
            cur.execute(s)
        conn.commit()
    finally:
        cur.close()
        conn.close()

def load_dim_date():
    conn = get_db_connection('finance_dwh')
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM dwh.DimDate")
        if cur.fetchone()[0] == 0:
            dates = pd.date_range(start=datetime(2015,1,1), end=datetime(2035,12,31), freq='D')
            df = pd.DataFrame({
                'date_key':       [int(d.strftime('%Y%m%d')) for d in dates],
                'date':            dates,
                'day':             dates.day,
                'day_of_week':     dates.weekday,
                'day_name':        dates.day_name(),
                'month':           dates.month,
                'month_name':      dates.month_name(),
                'quarter':         dates.quarter,
                'year':            dates.year,
                'is_weekend':      dates.weekday >= 5,
                'is_holiday':      False
            })[[
              'date_key','date','day','day_of_week','day_name',
              'month','month_name','quarter','year','is_weekend','is_holiday'
            ]]
            buf = StringIO()
            df.to_csv(buf, index=False, header=False, sep='\t')
            buf.seek(0)
            cur.copy_expert("COPY dwh.DimDate FROM STDIN WITH CSV DELIMITER E'\t'", buf)
            conn.commit()
    finally:
        cur.close()
        conn.close()

def load_dim_portfolio():
    src = get_db_connection('finance_clean_db')
    df = pd.read_sql_query("SELECT * FROM clean_data.Portfolios", src)
    src.close()

    # normalize keys
    df['portfolio_id'] = df['portfolio_id'].astype(str).str.strip().str.upper()
    df['target_return'] = df['target_return'].apply(lambda x: pd.to_numeric(str(x).strip('%'), errors='coerce')/100)

    dwh = get_db_connection('finance_dwh')
    date_map = pd.read_sql_query("SELECT date, date_key FROM dwh.DimDate", dwh).set_index('date')['date_key']
    df['inception_date_key']     = df['inception_date'].map(date_map)
    df['last_rebalance_date_key'] = df['last_rebalance_date'].map(date_map)

    df = df.rename(columns={'inception_date':'drop1','last_rebalance_date':'drop2'}).drop(['drop1','drop2'], axis=1)[[
      'portfolio_id','portfolio_name','portfolio_type','management_style',
      'risk_tolerance','target_return','inception_date_key',
      'last_rebalance_date_key','benchmark_id','status'
    ]]

    with dwh.cursor() as cur:
        cur.execute("TRUNCATE dwh.stg_dimportfolio;")
        buf = StringIO(); df.to_csv(buf,index=False,header=False,sep='\t'); buf.seek(0)
        cur.copy_expert("COPY dwh.stg_dimportfolio FROM STDIN WITH CSV DELIMITER E'\t'", buf)
        cur.execute("""
            INSERT INTO dwh.DimPortfolio AS tgt (
              portfolio_id, portfolio_name, portfolio_type, management_style,
              risk_tolerance, target_return, inception_date_key,
              last_rebalance_date_key, benchmark_id, status
            )
            SELECT * FROM dwh.stg_dimportfolio
            ON CONFLICT (portfolio_id) DO UPDATE SET
              portfolio_name          = EXCLUDED.portfolio_name,
              portfolio_type          = EXCLUDED.portfolio_type,
              management_style        = EXCLUDED.management_style,
              risk_tolerance          = EXCLUDED.risk_tolerance,
              target_return           = EXCLUDED.target_return,
              inception_date_key      = EXCLUDED.inception_date_key,
              last_rebalance_date_key = EXCLUDED.last_rebalance_date_key,
              benchmark_id            = EXCLUDED.benchmark_id,
              status                  = EXCLUDED.status;
        """)
        dwh.commit()
    dwh.close()

def load_dim_customer():
    src = get_db_connection('finance_clean_db')
    df = pd.read_sql_query("""
      SELECT c.customer_id,
             c.first_name||' '||c.last_name AS full_name,
             c.email, c.city, c.state, c.country,
             s.segment_name, s.annual_income, s.investment_goal,
             s.investment_horizon, s.risk_profile_id,
             c.registration_date, c.is_active
      FROM clean_data.Customers c
      LEFT JOIN clean_data.CustomerSegments s ON c.customer_id=s.customer_id
    """, src)
    src.close()

    # normalize key
    df['customer_id'] = df['customer_id'].astype(str).str.strip().str.upper()

    dwh = get_db_connection('finance_dwh')
    date_map = pd.read_sql_query("SELECT date, date_key FROM dwh.DimDate", dwh).set_index('date')['date_key']
    df['registration_date_key'] = df['registration_date'].map(date_map)

    df['risk_profile_id'] = pd.to_numeric(df['risk_profile_id'], errors='coerce').fillna(0).astype(int)
    df['is_active'] = df['is_active'].fillna(False)

    df = df.drop(['registration_date'], axis=1)[[
      'customer_id','full_name','email','city','state','country',
      'segment_name','annual_income','investment_goal',
      'investment_horizon','risk_profile_id','registration_date_key','is_active'
    ]].drop_duplicates(subset=['customer_id'])

    with dwh.cursor() as cur:
        cur.execute("TRUNCATE dwh.stg_dimcustomer;")
        buf = StringIO(); df.to_csv(buf,index=False,header=False,sep='\t'); buf.seek(0)
        cur.copy_expert("COPY dwh.stg_dimcustomer FROM STDIN WITH CSV DELIMITER E'\t'", buf)
        cur.execute("""
            INSERT INTO dwh.DimCustomer AS tgt (
              customer_id, full_name, email, city, state, country,
              segment_name, annual_income, investment_goal,
              investment_horizon, risk_profile_id, registration_date_key, is_active
            )
            SELECT * FROM dwh.stg_dimcustomer
            ON CONFLICT (customer_id) DO UPDATE SET
              full_name            = EXCLUDED.full_name,
              email                = EXCLUDED.email,
              city                 = EXCLUDED.city,
              state                = EXCLUDED.state,
              country              = EXCLUDED.country,
              segment_name         = EXCLUDED.segment_name,
              annual_income        = EXCLUDED.annual_income,
              investment_goal      = EXCLUDED.investment_goal,
              investment_horizon   = EXCLUDED.investment_horizon,
              risk_profile_id      = EXCLUDED.risk_profile_id,
              registration_date_key= EXCLUDED.registration_date_key,
              is_active            = EXCLUDED.is_active;
        """)
        dwh.commit()
    dwh.close()

def load_dim_advisor():
    src = get_db_connection('finance_clean_db')
    df = pd.read_sql_query("""
      SELECT a.advisor_id,
             a.first_name||' '||a.last_name AS full_name,
             a.title, a.branch_id, a.department,
             a.years_experience, a.certification,
             t.team_id, t.team_name, a.status
      FROM clean_data.Advisors a
      LEFT JOIN clean_data.TeamMembers tm ON a.advisor_id=tm.advisor_id
      LEFT JOIN clean_data.Teams t ON tm.team_id=t.team_id
    """, src).drop_duplicates('advisor_id')
    src.close()

    df['advisor_id'] = df['advisor_id'].astype(str).str.strip().str.upper()

    dwh = get_db_connection('finance_dwh')
    with dwh.cursor() as cur:
        cur.execute("TRUNCATE dwh.stg_dimadvisor;")
        buf = StringIO(); df.to_csv(buf,index=False,header=False,sep='\t'); buf.seek(0)
        cur.copy_expert("COPY dwh.stg_dimadvisor FROM STDIN WITH CSV DELIMITER E'\t'", buf)
        cur.execute("""
            INSERT INTO dwh.DimAdvisor AS tgt (
              advisor_id, full_name, title, branch_id, department,
              years_experience, certification, team_id, team_name, status
            )
            SELECT * FROM dwh.stg_dimadvisor
            ON CONFLICT (advisor_id) DO UPDATE SET
              full_name         = EXCLUDED.full_name,
              title             = EXCLUDED.title,
              branch_id         = EXCLUDED.branch_id,
              department        = EXCLUDED.department,
              years_experience  = EXCLUDED.years_experience,
              certification     = EXCLUDED.certification,
              team_id           = EXCLUDED.team_id,
              team_name         = EXCLUDED.team_name,
              status            = EXCLUDED.status;
        """)
        dwh.commit()
    dwh.close()

def load_dim_instrument_holding():
    src = get_db_connection('finance_clean_db')
    df = pd.read_sql_query("""
      SELECT ph.portfolio_id, ph.instrument_id, ph.purchase_date, ph.weight,
             i.name AS instrument_name, i.ticker, i.instrument_type,
             s.sector_name, ind.industry_name
      FROM clean_data.portfolioholdings ph
      LEFT JOIN clean_data.investmentinstruments i ON ph.instrument_id=i.instrument_id
      LEFT JOIN clean_data.sectors s ON i.sector_id=s.sector_id
      LEFT JOIN clean_data.industries ind ON i.industry_id=ind.industry_id
    """, src)
    src.close()

    # normalize
    df['instrument_id'] = df['instrument_id'].astype(str).str.strip().str.upper()

    # ─── DEDUPE ────────────────────────────────────────────────────────────────
    # Keep only the last occurrence for each (portfolio_id, instrument_id)
    df = df.sort_values(['portfolio_id', 'instrument_id', 'purchase_date']) \
           .drop_duplicates(subset=['portfolio_id','instrument_id'], keep='last')
    # ──────────────────────────────────────────────────────────────────────────

    print("▶︎ InstrumentHolding rows after dedupe:", len(df))
    print(df[['instrument_id','instrument_name','ticker']].drop_duplicates().head())

    dwh = get_db_connection('finance_dwh')
    date_map = pd.read_sql_query("SELECT date, date_key FROM dwh.DimDate", dwh).set_index('date')['date_key']
    df['purchase_date_key'] = df['purchase_date'].map(date_map)

    df = df[[
      'portfolio_id','instrument_id','instrument_name','ticker',
      'instrument_type','sector_name','industry_name','weight','purchase_date_key'
    ]]

    with dwh.cursor() as cur:
        cur.execute("TRUNCATE dwh.stg_diminstrumentholding;")
        buf = StringIO(); df.to_csv(buf,index=False,header=False,sep='\t'); buf.seek(0)
        cur.copy_expert("COPY dwh.stg_diminstrumentholding FROM STDIN WITH CSV DELIMITER E'\t'", buf)
        cur.execute("""
            INSERT INTO dwh.DimInstrumentHolding AS tgt (
              portfolio_id, instrument_id, instrument_name, ticker,
              instrument_type, sector_name, industry_name, weight, purchase_date_key
            )
            SELECT * FROM dwh.stg_diminstrumentholding
            ON CONFLICT (portfolio_id, instrument_id) DO UPDATE SET
              instrument_name    = EXCLUDED.instrument_name,
              ticker             = EXCLUDED.ticker,
              instrument_type    = EXCLUDED.instrument_type,
              sector_name        = EXCLUDED.sector_name,
              industry_name      = EXCLUDED.industry_name,
              weight             = EXCLUDED.weight,
              purchase_date_key  = EXCLUDED.purchase_date_key;
        """)
        dwh.commit()
    dwh.close()

def load_fact_portfolio_performance():
    src = get_db_connection('finance_clean_db')
    df = pd.read_sql_query("""
      SELECT pp.date, pp.portfolio_id, p.customer_id, p.advisor_id,
             pp.market_value, pp.deposits, pp.withdrawals,
             pp.daily_return_percentage, pp.cumulative_return,
             pp.benchmark_return, pp.alpha, pp.beta,
             pp.sharpe_ratio, pp.volatility
      FROM clean_data.PortfolioPerformance pp
      JOIN clean_data.Portfolios p ON pp.portfolio_id=p.portfolio_id
    """, src)
    src.close()

    # normalize keys
    for col in ['portfolio_id','customer_id','advisor_id']:
        df[col] = df[col].astype(str).str.strip().str.upper()

    print("▶︎ Fact raw rows:", len(df))
    print(df[['portfolio_id','customer_id','advisor_id']].drop_duplicates().head())

    dwh = get_db_connection('finance_dwh')
    #date_map = pd.read_sql_query("SELECT date, date_key FROM dwh.DimDate", dwh).set_index('date')['date_key']
    #df['date_key'] = df['date'].map(date_map)
    df['date'] = pd.to_datetime(df['date'])
    dim_date = pd.read_sql_query("SELECT date, date_key FROM dwh.DimDate", dwh)
    dim_date['date'] = pd.to_datetime(dim_date['date'])
    date_map = dim_date.set_index('date')['date_key']
    df['date_key'] = df['date'].map(date_map)
    
    missing = df['date_key'].isna().sum()

    df = df.dropna(subset=['date_key'])
    df['date_key'] = df['date_key'].astype(int)
    

    stg = df[[
      'date_key','portfolio_id','customer_id','advisor_id',
      'market_value','deposits','withdrawals',
      'daily_return_percentage','cumulative_return','benchmark_return',
      'alpha','beta','sharpe_ratio','volatility'
    ]]
    
    stg = (stg.sort_values(['date_key','portfolio_id'], ascending=[True, True]).drop_duplicates(subset=['date_key','portfolio_id'], keep='last')) 

    with dwh.cursor() as cur:
        cur.execute("TRUNCATE dwh.stg_factperformance;")
        buf = StringIO(); stg.to_csv(buf,index=False,header=False,sep='\t'); buf.seek(0)
        cur.copy_expert("COPY dwh.stg_factperformance FROM STDIN WITH CSV DELIMITER E'\t'", buf)

        # Debug missing dimension matches
        cur.execute("""
            SELECT DISTINCT st.portfolio_id 
            FROM dwh.stg_factperformance st 
            LEFT JOIN dwh.DimPortfolio p ON st.portfolio_id=p.portfolio_id
            WHERE p.portfolio_key IS NULL
            LIMIT 5
        """)
        missing_portfolios = cur.fetchall()
        if missing_portfolios:
            print("⚠️ Missing portfolio matches:", missing_portfolios)
            
        cur.execute("""
            SELECT DISTINCT st.customer_id 
            FROM dwh.stg_factperformance st 
            LEFT JOIN dwh.DimCustomer c ON st.customer_id=c.customer_id
            WHERE c.customer_key IS NULL
            LIMIT 5
        """)
        missing_customers = cur.fetchall()
        if missing_customers:
            print("⚠️ Missing customer matches:", missing_customers)
            
        cur.execute("""
            SELECT DISTINCT st.advisor_id 
            FROM dwh.stg_factperformance st 
            LEFT JOIN dwh.DimAdvisor a ON st.advisor_id=a.advisor_id
            WHERE a.advisor_key IS NULL
            LIMIT 5
        """)
        missing_advisors = cur.fetchall()
        if missing_advisors:
            print("⚠️ Missing advisor matches:", missing_advisors)
            
        # Continue with insert regardless of matches to see the records that do match
        cur.execute("""
            INSERT INTO dwh.FactPortfolioPerformance AS tgt (
              date_key, portfolio_key, customer_key, advisor_key,
              market_value, deposits, withdrawals,
              daily_return_percentage, cumulative_return, benchmark_return,
              alpha, beta, sharpe_ratio, volatility
            )
            SELECT
              st.date_key,
              p.portfolio_key,
              c.customer_key,
              a.advisor_key,
              st.market_value, st.deposits, st.withdrawals,
              st.daily_return_percentage, st.cumulative_return, st.benchmark_return,
              st.alpha, st.beta, st.sharpe_ratio, st.volatility
            FROM dwh.stg_factperformance st
            JOIN dwh.DimPortfolio p ON st.portfolio_id=p.portfolio_id
            JOIN dwh.DimCustomer c ON st.customer_id=c.customer_id
            JOIN dwh.DimAdvisor a ON st.advisor_id=a.advisor_id
            ON CONFLICT (date_key, portfolio_key) DO UPDATE SET
              customer_key            = EXCLUDED.customer_key,
              advisor_key             = EXCLUDED.advisor_key,
              market_value            = EXCLUDED.market_value,
              deposits                = EXCLUDED.deposits,
              withdrawals             = EXCLUDED.withdrawals,
              daily_return_percentage = EXCLUDED.daily_return_percentage,
              cumulative_return       = EXCLUDED.cumulative_return,
              benchmark_return        = EXCLUDED.benchmark_return,
              alpha                   = EXCLUDED.alpha,
              beta                    = EXCLUDED.beta,
              sharpe_ratio            = EXCLUDED.sharpe_ratio,
              volatility              = EXCLUDED.volatility;
        """)
        
        # Check how many rows were inserted
        cur.execute("SELECT COUNT(*) FROM dwh.FactPortfolioPerformance")
        row_count = cur.fetchone()[0]
        print(f"▶︎ FactPortfolioPerformance now has {row_count} rows")
        
        dwh.commit()
    dwh.close()

with DAG(
    dag_id='finance_data_warehouse_etl',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025,5,15),
    catchup=False,
    tags=['finance','dwh','etl']
) as dag:

    t1 = PythonOperator(task_id='create_schema',                    python_callable=create_dwh_schema)
    t2 = PythonOperator(task_id='load_dim_date',                    python_callable=load_dim_date)
    t3 = PythonOperator(task_id='load_dim_portfolio',               python_callable=load_dim_portfolio)
    t4 = PythonOperator(task_id='load_dim_customer',                python_callable=load_dim_customer)
    t5 = PythonOperator(task_id='load_dim_advisor',                 python_callable=load_dim_advisor)
    t6 = PythonOperator(task_id='load_dim_instrument_holding',      python_callable=load_dim_instrument_holding)
    t7 = PythonOperator(task_id='load_fact_portfolio_performance',  python_callable=load_fact_portfolio_performance)

    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7

