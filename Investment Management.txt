"""
Investment Management Database Generator
Creates a SQLite database with tables and realistic data based on provided schema.
Generates 10,000-20,000 rows for each table.
"""

import sqlite3
import random
import datetime
import time
import os
import string
from faker import Faker
from tqdm import tqdm
import numpy as np

# Initialize Faker for generating realistic data
fake = Faker()
Faker.seed(42)  # For reproducibility
random.seed(42)
np.random.seed(42)

# Database file
DB_FILE = 'investment.db'

# Delete existing database if it exists
if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

# Connect to database
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Function to execute queries with error handling
def execute_query(query, params=None):
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        print(f"Query: {query}")
        if params:
            print(f"Params: {params}")
        raise

# # Function to generate random date between start and end
# def random_date(start_date, end_date):
#     time_between = end_date - start_date
#     days_between = time_between.days
#     random_days = random.randrange(days_between)
#     return start_date + datetime.timedelta(days=random_days)




def random_date(start_date, end_date):
    # ensure both bounds are datetime.datetime
    if isinstance(start_date, datetime.date) and not isinstance(start_date, datetime.datetime):
        start_date = datetime.datetime.combine(start_date, datetime.time.min)
    if isinstance(end_date, datetime.date) and not isinstance(end_date, datetime.datetime):
        end_date = datetime.datetime.combine(end_date,   datetime.time.max)

    delta = end_date - start_date
    # pick a random number of seconds in that interval
    random_second = random.randrange(int(delta.total_seconds()))
    return start_date + datetime.timedelta(seconds=random_second)





# Function to generate random datetime between start and end
def random_datetime(start_date, end_date):
    random_date_obj = random_date(start_date, end_date)
    random_hour = random.randint(0, 23)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)
    return datetime.datetime(
        random_date_obj.year,
        random_date_obj.month,
        random_date_obj.day,
        random_hour, random_minute, random_second
    )

# Function to generate random time string in HH:MM format
def random_time():
    hours = random.randint(0, 23)
    minutes = random.randint(0, 59)
    return f"{hours:02d}:{minutes:02d}"

# Function to generate random alphanumeric ID of specific length
def generate_id(prefix, length):
    chars = string.ascii_uppercase + string.digits
    random_part = ''.join(random.choice(chars) for _ in range(length-len(prefix)))
    return f"{prefix}{random_part}"

# Function to convert date to string
def date_to_str(date_obj):
    return date_obj.strftime('%Y-%m-%d')

# Function to convert datetime to string
def datetime_to_str(datetime_obj):
    return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')

# Function to generate random decimal with specified precision
def random_decimal(min_val, max_val, decimal_places=2):
    val = round(random.uniform(min_val, max_val), decimal_places)
    return val

# Define constants
NUM_CUSTOMERS = random.randint(10000, 20000)
NUM_CUSTOMER_SEGMENTS = random.randint(10000, 20000)
NUM_CLIENT_CONTACTS = random.randint(10000, 20000)
NUM_INVESTMENT_INSTRUMENTS = random.randint(10000, 20000)
NUM_INSTRUMENT_PRICES = random.randint(10000, 20000)
NUM_INSTRUMENT_RATINGS = random.randint(10000, 20000)
NUM_MARKETS = random.randint(500, 1000)  # Fewer markets in reality
NUM_ECONOMIC_INDICATORS = random.randint(500, 1000)  # Fewer indicators in reality
NUM_ECONOMIC_INDICATOR_VALUES = random.randint(10000, 20000)
NUM_EXCHANGE_RATES = random.randint(10000, 20000)
NUM_PORTFOLIOS = random.randint(10000, 20000)
NUM_PORTFOLIO_HOLDINGS = random.randint(10000, 20000)
NUM_PORTFOLIO_TRANSACTIONS = random.randint(10000, 20000)
NUM_PORTFOLIO_PERFORMANCE = random.randint(10000, 20000)
NUM_ADVISORS = random.randint(1000, 2000)  # Fewer advisors in reality
NUM_TEAMS = random.randint(500, 1000)  # Fewer teams in reality
NUM_TEAM_MEMBERS = random.randint(10000, 20000)
NUM_BRANCHES = random.randint(500, 1000)  # Fewer branches in reality
NUM_SECTORS = random.randint(20, 50)  # Fewer sectors in reality
NUM_INDUSTRIES = random.randint(100, 200)  # Fewer industries in reality
NUM_COUNTRIES = random.randint(150, 200)  # Match real world count
NUM_REGIONS = random.randint(10, 20)  # Fewer regions in reality

# Date ranges
START_DATE = datetime.date(2015, 1, 1)
END_DATE = datetime.date(2025, 5, 10)  # Current date as of script creation

print("Creating tables...")

# Create supporting tables first for referential integrity
# Regions table
execute_query('''
CREATE TABLE Regions (
    region_id INTEGER PRIMARY KEY,
    region_name VARCHAR(100),
    region_code VARCHAR(20),
    continent VARCHAR(50),
    description TEXT
)
''')

# Countries table
execute_query('''
CREATE TABLE Countries (
country_id INTEGER PRIMARY KEY,
     country_name VARCHAR(100),
     country_code VARCHAR(10),
     region_id INTEGER,
     currency_code VARCHAR(10),
     timezone VARCHAR(50),
     is_active TINYINT,
     FOREIGN KEY(region_id) REFERENCES Regions(region_id)
)
''')

# Sectors table
execute_query('''
CREATE TABLE Sectors (
    sector_id INTEGER PRIMARY KEY,
    sector_name VARCHAR(100),
    sector_code VARCHAR(20),
    description TEXT,
    parent_sector_id INTEGER
)
''')

# Industries table
execute_query('''
CREATE TABLE Industries (
    industry_id INTEGER PRIMARY KEY,
    industry_name VARCHAR(100),
    industry_code VARCHAR(20),
    sector_id INTEGER,
    description TEXT
)
''')

# Branches table
execute_query('''
CREATE TABLE Branches (
    branch_id INTEGER PRIMARY KEY,
    branch_name VARCHAR(100),
    branch_code VARCHAR(20),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    phone VARCHAR(20),
    manager_id VARCHAR(20),
    opening_date DATE,
    closing_date DATE,
    status VARCHAR(20)
)
''')

# Advisors table
execute_query('''
CREATE TABLE Advisors (
    advisor_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    hire_date DATE,
    termination_date DATE,
    title VARCHAR(100),
    department VARCHAR(50),
    manager_id VARCHAR(20),
    branch_id INTEGER,
    certification VARCHAR(255),
    education VARCHAR(255),
    years_experience INTEGER,
    status VARCHAR(20)
)
''')

# Teams table
execute_query('''
CREATE TABLE Teams (
    team_id INTEGER PRIMARY KEY,
    team_name VARCHAR(100),
    team_lead_id VARCHAR(20),
    department_id INTEGER,
    formation_date DATE,
    dissolution_date DATE,
    description TEXT,
    status VARCHAR(20)
)
''')

# TeamMembers table
execute_query('''
CREATE TABLE TeamMembers (
    team_member_id INTEGER PRIMARY KEY,
    team_id INTEGER,
    advisor_id VARCHAR(20),
    role VARCHAR(50),
    start_date DATE,
    end_date DATE,
    allocation_percentage DECIMAL(5,2)
)
''')

# Markets table
execute_query('''
CREATE TABLE Markets (
    market_id INTEGER PRIMARY KEY,
    market_name VARCHAR(100),
    market_code VARCHAR(20),
    region VARCHAR(50),
    country VARCHAR(50),
    timezone VARCHAR(50),
    open_time VARCHAR(20),
    close_time VARCHAR(20),
    currency VARCHAR(10),
    description TEXT
)
''')

# EconomicIndicators table
execute_query('''
CREATE TABLE EconomicIndicators (
    indicator_id INTEGER PRIMARY KEY,
    indicator_name VARCHAR(100),
    indicator_type VARCHAR(50),
    region_id INTEGER,
    country_id INTEGER,
    frequency VARCHAR(20),
    unit VARCHAR(20),
    source VARCHAR(100),
    is_seasonally_adjusted TINYINT,
    notes TEXT
)
''')

# Customer tables
execute_query('''
CREATE TABLE Customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    date_of_birth VARCHAR(20),
    gender VARCHAR(10),
    address VARCHAR(200),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    registration_date DATETIME,
    last_login TIMESTAMP,
    referral_source VARCHAR(50),
    is_active TINYINT
)
''')

# CustomerSegments table
execute_query('''
CREATE TABLE CustomerSegments (
    segment_id INTEGER PRIMARY KEY,
    customer_id VARCHAR(20),
    segment_name VARCHAR(50),
    annual_income DECIMAL(12,2),
    net_worth VARCHAR(50),
    profession VARCHAR(100),
    risk_profile_id INTEGER,
    investment_goal VARCHAR(100),
    investment_horizon VARCHAR(50),
    last_updated DATETIME
)
''')

# ClientContacts table
execute_query('''
CREATE TABLE ClientContacts (
    contact_id INTEGER PRIMARY KEY,
    customer_id VARCHAR(20),
    contact_date VARCHAR(30),
    contact_type VARCHAR(50),
    contact_purpose VARCHAR(100),
    contact_outcome VARCHAR(255),
    follow_up_required VARCHAR(5),
    follow_up_date DATE,
    notes TEXT,
    advisor_id VARCHAR(20),
    satisfaction_score VARCHAR(10)
)
''')

# InvestmentInstruments table
execute_query('''
CREATE TABLE InvestmentInstruments (
    instrument_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    ticker VARCHAR(20),
    instrument_type VARCHAR(50),
    isin VARCHAR(20),
    cusip VARCHAR(20),
    issuer VARCHAR(100),
    issue_date DATE,
    maturity_date DATE,
    face_value DECIMAL(15,2),
    currency VARCHAR(10),
    sector_id INTEGER,
    industry_id INTEGER,
    price_source VARCHAR(50)
)
''')

# InstrumentPrices table
execute_query('''
CREATE TABLE InstrumentPrices (
    price_id INTEGER PRIMARY KEY,
    instrument_id VARCHAR(20),
    price_date DATE,
    open_price DECIMAL(15,4),
    high_price DECIMAL(15,4),
    low_price DECIMAL(15,4),
    close_price VARCHAR(20),
    adjusted_close DECIMAL(15,4),
    volume VARCHAR(20),
    market_cap DECIMAL(20,2),
    dividend DECIMAL(10,4),
    split_ratio DECIMAL(10,4),
    data_source VARCHAR(50),
    timestamp TIMESTAMP
)
''')

# InstrumentRatings table
execute_query('''
CREATE TABLE InstrumentRatings (
    rating_id INTEGER PRIMARY KEY,
    instrument_id VARCHAR(20),
    rating_agency VARCHAR(50),
    rating_value VARCHAR(20),
    rating_date DATE,
    previous_rating VARCHAR(20),
    outlook VARCHAR(20),
    report_url VARCHAR(255),
    is_investment_grade VARCHAR(5)
)
''')

# EconomicIndicatorValues table
execute_query('''
CREATE TABLE EconomicIndicatorValues (
    value_id INTEGER PRIMARY KEY,
    indicator_id INTEGER,
    date DATE,
    value VARCHAR(20),
    previous_value DECIMAL(15,4),
    percentage_change VARCHAR(10),
    forecast_value DECIMAL(15,4),
    actual_release_date DATETIME,
    next_release_date DATE
)
''')

# ExchangeRates table
execute_query('''
CREATE TABLE ExchangeRates (
    rate_id INTEGER PRIMARY KEY,
    from_currency VARCHAR(10),
    to_currency VARCHAR(10),
    rate_date DATE,
    exchange_rate DECIMAL(15,6),
    inverse_rate VARCHAR(20),
    data_source VARCHAR(50)
)
''')

# Portfolios table
execute_query('''
CREATE TABLE Portfolios (
    portfolio_id VARCHAR(20) PRIMARY KEY,
    portfolio_name VARCHAR(100),
    customer_id VARCHAR(20),
    advisor_id VARCHAR(20),
    inception_date DATE,
    portfolio_type VARCHAR(50),
    benchmark_id VARCHAR(20),
    management_style VARCHAR(50),
    risk_tolerance VARCHAR(20),
    target_return VARCHAR(20),
    status VARCHAR(20),
    last_rebalance_date DATE
)
''')

# PortfolioHoldings table
execute_query('''
CREATE TABLE PortfolioHoldings (
    holding_id INTEGER PRIMARY KEY,
    portfolio_id VARCHAR(20),
    instrument_id VARCHAR(20),
    purchase_date DATE,
    quantity DECIMAL(15,4),
    purchase_price DECIMAL(15,4),
    current_price DECIMAL(15,4),
    market_value VARCHAR(20),
    weight DECIMAL(6,4),
    currency VARCHAR(10),
    notes TEXT,
    last_updated TIMESTAMP
)
''')

# PortfolioTransactions table
execute_query('''
CREATE TABLE PortfolioTransactions (
    transaction_id VARCHAR(30) PRIMARY KEY,
    portfolio_id VARCHAR(20),
    instrument_id VARCHAR(20),
    transaction_date VARCHAR(30),
    transaction_type VARCHAR(20),
    quantity DECIMAL(15,4),
    price DECIMAL(15,4),
    amount VARCHAR(20),
    fees DECIMAL(10,2),
    taxes DECIMAL(10,2),
    currency VARCHAR(10),
    settlement_date DATE,
    broker_id VARCHAR(20),
    notes TEXT
)
''')

# PortfolioPerformance table
execute_query('''
CREATE TABLE PortfolioPerformance (
    performance_id INTEGER PRIMARY KEY,
    portfolio_id VARCHAR(20),
    date DATE,
    market_value DECIMAL(15,2),
    deposits DECIMAL(15,2),
    withdrawals DECIMAL(15,2),
    daily_return VARCHAR(10),
    daily_return_percentage DECIMAL(10,6),
    cumulative_return DECIMAL(15,6),
    benchmark_return DECIMAL(10,6),
    alpha VARCHAR(10),
    beta VARCHAR(10),
    sharpe_ratio VARCHAR(10),
    volatility DECIMAL(10,6)
)
''')

print("Tables created successfully!")

# Define data generation constants for realistic data
GENDERS = ['Male', 'Female', 'Other', 'Prefer not to say']
SEGMENT_NAMES = ['High Net Worth', 'Mass Affluent', 'Retail', 'Ultra High Net Worth', 'Institutional',
                'Corporate', 'Small Business', 'Retiree', 'Young Professional', 'Student']
NET_WORTH_RANGES = ['Under $100K', '$100K-$250K', '$250K-$500K', '$500K-$1M', '$1M-$5M', '$5M-$10M',
                   '$10M-$50M', '$50M-$100M', '$100M+']
PROFESSIONS = ['Doctor', 'Lawyer', 'Engineer', 'Banker', 'Teacher', 'Professor', 'Business Owner',
               'Executive', 'Artist', 'Writer', 'IT Professional', 'Consultant', 'Accountant',
               'Retired', 'Student', 'Healthcare Professional', 'Sales Representative', 'Marketing Professional']
RISK_PROFILES = list(range(1, 11))  # 1 to 10
INVESTMENT_GOALS = ['Retirement', 'College Education', 'Capital Preservation', 'Income Generation',
                   'Wealth Accumulation', 'Legacy Planning', 'Tax Minimization', 'Home Purchase',
                   'Business Investment', 'Emergency Fund']
INVESTMENT_HORIZONS = ['Short-term (< 3 years)', 'Medium-term (3-7 years)', 'Long-term (7+ years)']
CONTACT_TYPES = ['Email', 'Phone', 'In-person', 'Video Conference', 'Letter', 'Text Message']
CONTACT_PURPOSES = ['Account Review', 'Portfolio Update', 'Market Information', 'Investment Opportunity',
                   'Financial Planning', 'Tax Planning', 'Estate Planning', 'Complaint Resolution',
                   'Administrative', 'New Client Onboarding']
CONTACT_OUTCOMES = ['Successful', 'Follow-up Required', 'No Answer', 'Left Message', 'Resolved Issue',
                   'Information Provided', 'New Investment Made', 'Account Updated', 'Meeting Scheduled']
YES_NO = ['Yes', 'No']
SATISFACTION_SCORES = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', 'N/A']
REFERRAL_SOURCES = ['Friend/Family', 'Online Search', 'Social Media', 'Advertisement', 'Event',
                   'Existing Client', 'Professional Referral', 'Cold Call', 'Website', 'Other']
INSTRUMENT_TYPES = ['Stock', 'Bond', 'ETF', 'Mutual Fund', 'Options', 'Futures', 'REIT', 'CD',
                   'Money Market', 'Commodity', 'Forex', 'Cryptocurrency', 'Structured Product']
CURRENCIES = ['USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'CNY', 'HKD', 'SGD', 'INR', 'BRL']
PRICE_SOURCES = ['Bloomberg', 'Reuters', 'IDC', 'Morningstar', 'Yahoo Finance', 'Internal', 'FactSet']
RATING_AGENCIES = ['S&P', 'Moody\'s', 'Fitch', 'DBRS', 'AM Best', 'Morningstar', 'Lipper', 'Internal Rating']
RATINGS = ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-', 'BB+', 'BB', 'BB-', 'B+', 'B', 'B-',
          'CCC+', 'CCC', 'CCC-', 'CC', 'C', 'D']
OUTLOOKS = ['Positive', 'Negative', 'Stable', 'Developing', 'Under Review']
MARKET_NAMES = ['New York Stock Exchange', 'NASDAQ', 'London Stock Exchange', 'Tokyo Stock Exchange',
               'Shanghai Stock Exchange', 'Hong Kong Stock Exchange', 'Euronext', 'Toronto Stock Exchange',
               'Frankfurt Stock Exchange', 'SIX Swiss Exchange']
ECONOMIC_INDICATOR_TYPES = ['GDP', 'Inflation', 'Unemployment', 'Interest Rate', 'Consumer Confidence',
                           'Housing Starts', 'Retail Sales', 'Industrial Production', 'PMI', 'Trade Balance']
FREQUENCIES = ['Daily', 'Weekly', 'Monthly', 'Quarterly', 'Semi-annually', 'Annually']
UNITS = ['Percentage', 'Index Points', 'Basis Points', 'Currency', 'Ratio', 'Number']
SOURCES = ['Bureau of Economic Analysis', 'Federal Reserve', 'Bureau of Labor Statistics', 'Census Bureau',
          'World Bank', 'IMF', 'OECD', 'Bloomberg', 'Reuters']
PORTFOLIO_TYPES = ['Conservative', 'Moderate', 'Aggressive', 'Income', 'Growth', 'Balanced', 'Tax-Advantaged',
                  'ESG/Sustainable', 'Sector-Specific', 'International']
MANAGEMENT_STYLES = ['Active', 'Passive', 'Blend', 'Core-Satellite', 'Factor-Based', 'Tactical']
RISK_TOLERANCES = ['Very Low', 'Low', 'Moderate', 'High', 'Very High']
TRANSACTION_TYPES = ['Buy', 'Sell', 'Dividend', 'Interest', 'Fee', 'Transfer In', 'Transfer Out', 'Corporate Action']
STATUSES = ['Active', 'Inactive', 'Pending', 'Closed', 'Suspended']
ADVISOR_TITLES = ['Investment Advisor', 'Financial Planner', 'Portfolio Manager', 'Wealth Manager',
                 'Relationship Manager', 'Financial Consultant', 'Investment Specialist', 'Private Banker']
DEPARTMENTS = ['Wealth Management', 'Investment Banking', 'Research', 'Operations', 'Compliance', 'Marketing',
              'Technology', 'Human Resources', 'Finance', 'Legal']
CERTIFICATIONS = ['CFA', 'CFP', 'CPA', 'ChFC', 'CIMA', 'CLU', 'RIA', 'Series 7', 'Series 63', 'Series 65', 'Series 66']
EDUCATIONS = ['Bachelor\'s Degree', 'Master\'s Degree', 'MBA', 'PhD', 'JD', 'CFA Charter', 'CFP Certification']
TEAM_ROLES = ['Team Lead', 'Investment Specialist', 'Client Advisor', 'Research Analyst', 'Support Staff', 'Administrative']
CONTINENTS = ['North America', 'South America', 'Europe', 'Asia', 'Africa', 'Australia', 'Antarctica']
SECTOR_NAMES = ['Technology', 'Healthcare', 'Financials', 'Consumer Discretionary', 'Consumer Staples',
               'Energy', 'Materials', 'Industrials', 'Utilities', 'Real Estate', 'Communication Services']
DATA_SOURCES = ['Bloomberg', 'Reuters', 'FactSet', 'Morningstar', 'S&P Capital IQ', 'Internal']
TIMEZONES = ['EST', 'CST', 'MST', 'PST', 'GMT', 'UTC', 'JST', 'AEST', 'CET']

print("Generating data...")

# Generate Regions data
regions = []
for i in range(1, NUM_REGIONS + 1):
    continent = random.choice(CONTINENTS)
    region = {
        'region_id': i,
        'region_name': f"{continent} Region {i}",
        'region_code': f"REG{i:03d}",
        'continent': continent,
        'description': f"Economic and geographic region covering {continent} area {i}"
    }
    regions.append(region)

print("Inserting Regions data...")
for region in tqdm(regions):
    execute_query('''
    INSERT INTO Regions (region_id, region_name, region_code, continent, description)
    VALUES (?, ?, ?, ?, ?)
    ''', (
        region['region_id'],
        region['region_name'],
        region['region_code'],
        region['continent'],
        region['description']
    ))
conn.commit()

# Generate Countries data
countries = []
for i in range(1, NUM_COUNTRIES + 1):
    region_obj = random.choice(regions)
    country = {
        'country_id': i,
        'country_name': fake.country(),
        'country_code': ''.join(random.choices(string.ascii_uppercase, k=2)),
        'region_id': region_obj['region_id'],	
        'currency_code': random.choice(CURRENCIES),
        'timezone': random.choice(TIMEZONES),
        'is_active': random.choice([0, 1])
    }
    countries.append(country)

print("Inserting Countries data...")
for country in tqdm(countries):
    execute_query('''
    INSERT INTO Countries (country_id, country_name, country_code, region_id, currency_code, timezone, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        country['country_id'],
        country['country_name'],
        country['country_code'],
        country['region_id'],
        country['currency_code'],
        country['timezone'],
        country['is_active']
    ))
conn.commit()

# Generate Sectors data
sectors = []
for i in range(1, NUM_SECTORS + 1):
    sector_name = SECTOR_NAMES[i % len(SECTOR_NAMES)] if i <= len(SECTOR_NAMES) else f"Sector {i}"
    parent_sector_id = random.randint(1, i-1) if i > 1 and random.random() < 0.3 else None

    sector = {
        'sector_id': i,
        'sector_name': sector_name,
        'sector_code': f"SEC{i:03d}",
        'description': f"Sector covering {sector_name} industries and businesses",
        'parent_sector_id': parent_sector_id
    }
    sectors.append(sector)

print("Inserting Sectors data...")
for sector in tqdm(sectors):
    execute_query('''
    INSERT INTO Sectors (sector_id, sector_name, sector_code, description, parent_sector_id)
    VALUES (?, ?, ?, ?, ?)
    ''', (
        sector['sector_id'],
        sector['sector_name'],
        sector['sector_code'],
        sector['description'],
        sector['parent_sector_id']
    ))
conn.commit()

# Generate Industries data
industries = []
for i in range(1, NUM_INDUSTRIES + 1):
    sector = random.choice(sectors)

    industry = {
        'industry_id': i,
        'industry_name': f"{sector['sector_name']} Industry {i}",
        'industry_code': f"IND{i:03d}",
        'sector_id': sector['sector_id'],
        'description': f"Industry within the {sector['sector_name']} sector"
    }
    industries.append(industry)

print("Inserting Industries data...")
for industry in tqdm(industries):
    execute_query('''
    INSERT INTO Industries (industry_id, industry_name, industry_code, sector_id, description)
    VALUES (?, ?, ?, ?, ?)
    ''', (
        industry['industry_id'],
        industry['industry_name'],
        industry['industry_code'],
        industry['sector_id'],
        industry['description']
    ))
conn.commit()

# Generate Branches data
branches = []
for i in range(1, NUM_BRANCHES + 1):
    country = random.choice(countries)
    opening_date = random_date(datetime.date(2000, 1, 1), END_DATE)

    # 5% chance of branch being closed
    if random.random() < 0.05:
        closing_date = random_date(opening_date, END_DATE)
        status = 'Closed'
    else:
        closing_date = None
        status = 'Active'

    branch = {
        'branch_id': i,
        'branch_name': f"{fake.city()} Branch",
        'branch_code': f"BR{i:04d}",
        'address': fake.street_address(),
        'city': fake.city(),
        'state': fake.state(),
        'zip_code': fake.zipcode(),
        'country': country['country_name'],
        'phone': fake.phone_number(),
        'manager_id': None,  # Will update later
        'opening_date': opening_date,
        'closing_date': closing_date,
        'status': status
    }
    branches.append(branch)

print("Inserting Branches data (without managers for now)...")
for branch in tqdm(branches):
    execute_query('''
    INSERT INTO Branches (branch_id, branch_name, branch_code, address, city, state, zip_code, country, phone, manager_id, opening_date, closing_date, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        branch['branch_id'],
        branch['branch_name'],
        branch['branch_code'],
        branch['address'],
        branch['city'],
        branch['state'],
        branch['zip_code'],
        branch['country'],
        branch['phone'],
        branch['manager_id'],
        date_to_str(branch['opening_date']),
        date_to_str(branch['closing_date']) if branch['closing_date'] else None,
        branch['status']
    ))
conn.commit()

# Generate Advisors data
seen_advisor_ids = set()
advisors = []
for i in range(1, NUM_ADVISORS + 1):
    # advisor_id = generate_id("ADV", 8)
    advisor_id = generate_id("ADV", 8)
    while advisor_id in seen_advisor_ids:
    	advisor_id = generate_id("ADV", 8)
    seen_advisor_ids.add(advisor_id)
    
    branch = random.choice(branches)
    hire_date = random_date(datetime.date(2000, 1, 1), END_DATE)

    # 10% chance of advisor being terminated
    if random.random() < 0.1:
        termination_date = random_date(hire_date, END_DATE)
        status = 'Inactive'
    else:
        termination_date = None
        status = 'Active'

    # Randomly select certifications
    num_certifications = random.randint(0, 3)
    certifications = ', '.join(random.sample(CERTIFICATIONS, num_certifications)) if num_certifications > 0 else None

    # Randomly select education
    education = random.choice(EDUCATIONS)

    # Years of experience (between 1 and 30)
    years_experience = random.randint(1, 30)

    advisor = {
        'advisor_id': advisor_id,
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.email(),
        'phone': fake.phone_number(),
        'hire_date': hire_date,
        'termination_date': termination_date,
        'title': random.choice(ADVISOR_TITLES),
        'department': random.choice(DEPARTMENTS),
        'manager_id': None,  # Will update some later
        'branch_id': branch['branch_id'],
        'certification': certifications,
        'education': education,
        'years_experience': years_experience,
        'status': status
    }
    advisors.append(advisor)

print("Inserting Advisors data...")
for advisor in tqdm(advisors):
    execute_query('''
    INSERT INTO Advisors (advisor_id, first_name, last_name, email, phone, hire_date, termination_date, title, department, manager_id, branch_id, certification, education, years_experience, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        advisor['advisor_id'],
        advisor['first_name'],
        advisor['last_name'],
        advisor['email'],
        advisor['phone'],
        date_to_str(advisor['hire_date']),
        date_to_str(advisor['termination_date']) if advisor['termination_date'] else None,
        advisor['title'],
        advisor['department'],
        advisor['manager_id'],
        advisor['branch_id'],
        advisor['certification'],
        advisor['education'],
        advisor['years_experience'],
        advisor['status']
    ))
conn.commit()

# Update some advisors to be managers of other advisors
active_advisors = [adv for adv in advisors if adv['status'] == 'Active']
for i in range(len(active_advisors)):
    if i > 0 and random.random() < 0.8:  # 80% of advisors have a manager
        manager = random.choice(active_advisors[:i])  # Choose from previously processed advisors
        active_advisors[i]['manager_id'] = manager['advisor_id']

        execute_query('''
        UPDATE Advisors
        SET manager_id = ?
        WHERE advisor_id = ?
        ''', (manager['advisor_id'], active_advisors[i]['advisor_id']))

# Update branch managers
for branch in branches:
    branch_advisors = [adv for adv in advisors if adv['branch_id'] == branch['branch_id'] and adv['status'] == 'Active']
    if branch_advisors:
        manager = random.choice(branch_advisors)
        branch['manager_id'] = manager['advisor_id']

        execute_query('''
        UPDATE Branches
        SET manager_id = ?
        WHERE branch_id = ?
        ''', (manager['advisor_id'], branch['branch_id']))
conn.commit()

# Generate Teams data
teams = []
departments = set([adv['department'] for adv in advisors])
department_ids = {dept: i for i, dept in enumerate(departments, 1)}

for i in range(1, NUM_TEAMS + 1):
    department = random.choice(list(departments))
    department_id = department_ids[department]
    active_advisors = [adv for adv in advisors if adv['status'] == 'Active']
    team_lead = random.choice(active_advisors)
    formation_date = random_date(team_lead['hire_date'], END_DATE)

    # 5% chance of team being dissolved
    if random.random() < 0.05:
        dissolution_date = random_date(formation_date, END_DATE)
        status = 'Inactive'
    else:
        dissolution_date = None
        status = 'Active'

    team = {
        'team_id': i,
        'team_name': f"{department} Team {i}",
        'team_lead_id': team_lead['advisor_id'],
        'department_id': department_id,
        'formation_date': formation_date,
        'dissolution_date': dissolution_date,
        'description': f"Team responsible for {department.lower()} activities",
        'status': status
    }
    teams.append(team)

print("Inserting Teams data...")
for team in tqdm(teams):
    execute_query('''
    INSERT INTO Teams (team_id, team_name, team_lead_id, department_id, formation_date, dissolution_date, description, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        team['team_id'],
        team['team_name'],
        team['team_lead_id'],
        team['department_id'],
        date_to_str(team['formation_date']),
        date_to_str(team['dissolution_date']) if team['dissolution_date'] else None,
        team['description'],
        team['status']
    ))
conn.commit()

# Generate TeamMembers data
team_members = []
for i in range(1, NUM_TEAM_MEMBERS + 1):
    active_teams = [t for t in teams if t['status'] == 'Active']
    if not active_teams:
        continue

    team = random.choice(active_teams)
    advisors_not_in_team = [adv for adv in advisors if adv['status'] == 'Active']

    if not advisors_not_in_team:
        continue

    advisor = random.choice(advisors_not_in_team)
    start_date = max(advisor['hire_date'], team['formation_date'])

    # 10% chance of member leaving team
    if random.random() < 0.1:
        end_date = random_date(start_date, END_DATE if not team['dissolution_date'] else team['dissolution_date'])
    else:
        end_date = team['dissolution_date']

    allocation_percentage = random.randint(10, 100)

    team_member = {
        'team_member_id': i,
        'team_id': team['team_id'],
        'advisor_id': advisor['advisor_id'],
        'role': random.choice(TEAM_ROLES),
        'start_date': start_date,
        'end_date': end_date,
        'allocation_percentage': allocation_percentage
    }
    team_members.append(team_member)

print("Inserting TeamMembers data...")
for member in tqdm(team_members):
    execute_query('''
    INSERT INTO TeamMembers (team_member_id, team_id, advisor_id, role, start_date, end_date, allocation_percentage)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        member['team_member_id'],
        member['team_id'],
        member['advisor_id'],
        member['role'],
        date_to_str(member['start_date']),
        date_to_str(member['end_date']) if member['end_date'] else None,
        member['allocation_percentage']
    ))
conn.commit()

# Generate Markets data
markets = []
for i in range(1, NUM_MARKETS + 1):
    country = random.choice(countries)
    region_obj = next((r for r in regions if r['region_id'] == country['region_id']), None)
    region = region_obj['region_name'] if region_obj else None

    # If i is less than the length of MARKET_NAMES, use a real market name
    if i <= len(MARKET_NAMES):
        market_name = MARKET_NAMES[i-1]
        market_code = ''.join([word[0] for word in market_name.split() if word[0].isalpha()]).upper()
    else:
        market_name = f"{country['country_name']} Stock Exchange {i}"
        market_code = f"MSE{i:03d}"

    open_time = random_time()
    close_time = random_time()

    market = {
        'market_id': i,
        'market_name': market_name,
        'market_code': market_code,
        'region': region,
        'country': country['country_name'],
        'timezone': country['timezone'],
        'open_time': open_time,
        'close_time': close_time,
        'currency': country['currency_code'],
        'description': f"Stock exchange operating in {country['country_name']}"
    }
    markets.append(market)

print("Inserting Markets data...")
for market in tqdm(markets):
    execute_query('''
    INSERT INTO Markets (market_id, market_name, market_code, region, country, timezone, open_time, close_time, currency, description)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        market['market_id'],
        market['market_name'],
        market['market_code'],
        market['region'],
        market['country'],
        market['timezone'],
        market['open_time'],
        market['close_time'],
        market['currency'],
        market['description']
    ))
conn.commit()

# Generate EconomicIndicators data
economic_indicators = []
for i in range(1, NUM_ECONOMIC_INDICATORS + 1):
    country = random.choice(countries)
    region_id = country['region_id']

    # For the first entries, use common economic indicators
    if i <= len(ECONOMIC_INDICATOR_TYPES):
        indicator_name = f"{country['country_name']} {ECONOMIC_INDICATOR_TYPES[i-1]}"
        indicator_type = ECONOMIC_INDICATOR_TYPES[i-1]
    else:
        indicator_name = f"{country['country_name']} Economic Indicator {i}"
        indicator_type = random.choice(ECONOMIC_INDICATOR_TYPES)

    indicator = {
        'indicator_id': i,
        'indicator_name': indicator_name,
        'indicator_type': indicator_type,
        'region_id': region_id,
        'country_id': country['country_id'],
        'frequency': random.choice(FREQUENCIES),
        'unit': random.choice(UNITS),
        'source': random.choice(SOURCES),
        'is_seasonally_adjusted': random.choice([0, 1]),
        'notes': f"Economic indicator tracking {indicator_type.lower()} for {country['country_name']}"
    }
    economic_indicators.append(indicator)

print("Inserting EconomicIndicators data...")
for indicator in tqdm(economic_indicators):
    execute_query('''
    INSERT INTO EconomicIndicators (indicator_id, indicator_name, indicator_type, region_id, country_id, frequency, unit, source, is_seasonally_adjusted, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        indicator['indicator_id'],
        indicator['indicator_name'],
        indicator['indicator_type'],
        indicator['region_id'],
        indicator['country_id'],
        indicator['frequency'],
        indicator['unit'],
        indicator['source'],
        indicator['is_seasonally_adjusted'],
        indicator['notes']
    ))
conn.commit()



# Generate Customers data
customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    # 1) Build a guaranteed-unique ID
    customer_id = f"CUST{i:08d}"           # e.g. CUST00000001, CUST00000002, …

    # 2) Pick a registration_date between START_DATE and END_DATE
    #    (assuming START_DATE and END_DATE are date or datetime.date objects)
    reg_date_date = fake.date_between_dates(
        date_start=START_DATE,
        date_end=END_DATE
    )
    registration_date = datetime.datetime.combine(
        reg_date_date,
        datetime.time.min
    )

    # 3) Last login sometime between registration and END_DATE
    last_login = random_datetime(
        registration_date,
        datetime.datetime.combine(END_DATE, datetime.time.max)
    )

    date_of_birth = fake.date_of_birth(
        minimum_age=18,
        maximum_age=90
    ).strftime('%Y-%m-%d')

    customers.append({
        'customer_id':      customer_id,
        'first_name':       fake.first_name(),
        'last_name':        fake.last_name(),
        'email':            fake.email(),
        'phone':            fake.phone_number(),
        'date_of_birth':    date_of_birth,
        'gender':           random.choice(GENDERS),
        'address':          fake.street_address(),
        'city':             fake.city(),
        'state':            fake.state(),
        'zip_code':         fake.zipcode(),
        'country':          random.choice(countries)['country_name'],
        'registration_date': registration_date,
        'last_login':       last_login,
        'referral_source':  random.choice(REFERRAL_SOURCES),
        'is_active':        random.choice([0, 1]),
    })

print("Inserting Customers data...")
for cust in tqdm(customers, desc="Customers"):
    execute_query('''
    INSERT INTO Customers (
        customer_id, first_name, last_name, email, phone,
        date_of_birth, gender, address, city, state, zip_code,
        country, registration_date, last_login, referral_source, is_active
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        cust['customer_id'],
        cust['first_name'],
        cust['last_name'],
        cust['email'],
        cust['phone'],
        cust['date_of_birth'],
        cust['gender'],
        cust['address'],
        cust['city'],
        cust['state'],
        cust['zip_code'],
        cust['country'],
        datetime_to_str(cust['registration_date']),
        datetime_to_str(cust['last_login']),
        cust['referral_source'],
        cust['is_active'],
    ))
conn.commit()


# Generate CustomerSegments data
customer_segments = []
for i in range(1, NUM_CUSTOMER_SEGMENTS + 1):
    customer = random.choice(customers)
    segment_name = random.choice(SEGMENT_NAMES)
    annual_income = random_decimal(20000, 1000000)
    risk_profile_id = random.choice(RISK_PROFILES)
    last_updated = random_datetime(customer['registration_date'], datetime.datetime.combine(END_DATE, datetime.datetime.min.time()))

    segment = {
        'segment_id': i,
        'customer_id': customer['customer_id'],
        'segment_name': segment_name,
        'annual_income': annual_income,
        'net_worth': random.choice(NET_WORTH_RANGES),
        'profession': random.choice(PROFESSIONS),
        'risk_profile_id': risk_profile_id,
        'investment_goal': random.choice(INVESTMENT_GOALS),
        'investment_horizon': random.choice(INVESTMENT_HORIZONS),
        'last_updated': last_updated
    }
    customer_segments.append(segment)

print("Inserting CustomerSegments data...")
for segment in tqdm(customer_segments):
    execute_query('''
    INSERT INTO CustomerSegments (segment_id, customer_id, segment_name, annual_income, net_worth, profession, risk_profile_id, investment_goal, investment_horizon, last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        segment['segment_id'],
        segment['customer_id'],
        segment['segment_name'],
        segment['annual_income'],
        segment['net_worth'],
        segment['profession'],
        segment['risk_profile_id'],
        segment['investment_goal'],
        segment['investment_horizon'],
        datetime_to_str(segment['last_updated'])
    ))
conn.commit()

# Generate ClientContacts data
client_contacts = []
for i in range(1, NUM_CLIENT_CONTACTS + 1):
    customer = random.choice(customers)
    active_advisors = [adv for adv in advisors if adv['status'] == 'Active']
    advisor = random.choice(active_advisors) if active_advisors else None

    contact_date = random_date(customer['registration_date'], END_DATE).strftime('%Y-%m-%d')

    follow_up_required = random.choice(YES_NO)
    if follow_up_required == 'Yes':
        follow_up_date = random_date(datetime.datetime.strptime(contact_date, '%Y-%m-%d').date(), END_DATE)
    else:
        follow_up_date = None

    contact = {
        'contact_id': i,
        'customer_id': customer['customer_id'],
        'contact_date': contact_date,
        'contact_type': random.choice(CONTACT_TYPES),
        'contact_purpose': random.choice(CONTACT_PURPOSES),
        'contact_outcome': random.choice(CONTACT_OUTCOMES),
        'follow_up_required': follow_up_required,
        'follow_up_date': follow_up_date,
        'notes': fake.paragraph(nb_sentences=3),
        'advisor_id': advisor['advisor_id'] if advisor else None,
        'satisfaction_score': random.choice(SATISFACTION_SCORES)
    }
    client_contacts.append(contact)

print("Inserting ClientContacts data...")
for contact in tqdm(client_contacts):
    execute_query('''
    INSERT INTO ClientContacts (contact_id, customer_id, contact_date, contact_type, contact_purpose, contact_outcome, follow_up_required, follow_up_date, notes, advisor_id, satisfaction_score)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        contact['contact_id'],
        contact['customer_id'],
        contact['contact_date'],
        contact['contact_type'],
        contact['contact_purpose'],
        contact['contact_outcome'],
        contact['follow_up_required'],
        date_to_str(contact['follow_up_date']) if contact['follow_up_date'] else None,
        contact['notes'],
        contact['advisor_id'],
        contact['satisfaction_score']
    ))
conn.commit()

import calendar



# 1) Pull all existing IDs from the DB into a set
cursor.execute("SELECT instrument_id FROM InvestmentInstruments")
seen_ids = {row[0] for row in cursor.fetchall()}

investment_instruments = []

# 2) Build your instruments list, never re-using an ID
for _ in range(NUM_INVESTMENT_INSTRUMENTS):
    # pick only from valid sectors/industries
    sector = random.choice([s for s in sectors if any(ind['sector_id']==s['sector_id'] for ind in industries)])
    industry = random.choice([ind for ind in industries if ind['sector_id']==sector['sector_id']])

    # guaranteed-unique ID
    while True:
        instrument_id = generate_id("INST", 8)
        if instrument_id not in seen_ids:
            seen_ids.add(instrument_id)
            break

    instrument_type = random.choice(['Bond', 'CD', 'Structured Product', 'Equity', 'ETF'])
    issue_date = random_date(datetime.date(2000, 1, 1), END_DATE)

    if instrument_type in ['Bond', 'CD', 'Structured Product']:
        # compute a “30-year” max maturity, clamped for Feb 29 etc.
        end_year = issue_date.year + 30
        month, day = issue_date.month, issue_date.day
        try:
            max_maturity = datetime.date(end_year, month, day)
        except ValueError:
            last_day = calendar.monthrange(end_year, month)[1]
            max_maturity = datetime.date(end_year, month, last_day)
        maturity_date = random_date(issue_date, max_maturity)
        face_value    = random_decimal(10, 1000)
    else:
        maturity_date = None
        face_value    = None

    investment_instruments.append({
        'instrument_id':   instrument_id,
        'name':            f"{fake.company()} {instrument_type}",
        'ticker':          ''.join(random.choices(string.ascii_uppercase, k=random.randint(3,5))),
        'instrument_type': instrument_type,
        'isin':            f"US{generate_id('', 10)}",
        'cusip':           generate_id('', 9),
        'issuer':          fake.company(),
        'issue_date':      issue_date,
        'maturity_date':   maturity_date,
        'face_value':      face_value,
        'currency':        random.choice(CURRENCIES),
        'sector_id':       sector['sector_id'],
        'industry_id':     industry['industry_id'],
        'price_source':    random.choice(PRICE_SOURCES)
    })

# 3) Insert them all
print("Inserting InvestmentInstruments data…")
for inst in tqdm(investment_instruments, desc="Instruments"):
    cursor.execute('''
        INSERT INTO InvestmentInstruments (
            instrument_id, name, ticker, instrument_type,
            isin, cusip, issuer, issue_date, maturity_date,
            face_value, currency, sector_id, industry_id, price_source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        inst['instrument_id'],
        inst['name'],
        inst['ticker'],
        inst['instrument_type'],
        inst['isin'],
        inst['cusip'],
        inst['issuer'],
        inst['issue_date'].isoformat(),
        inst['maturity_date'].isoformat() if inst['maturity_date'] else None,
        inst['face_value'],
        inst['currency'],
        inst['sector_id'],
        inst['industry_id'],
        inst['price_source'],
    ))

conn.commit()


# Generate InstrumentPrices data
instrument_prices = []
for i in range(1, NUM_INSTRUMENT_PRICES + 1):
    instrument = random.choice(investment_instruments)
    price_date = random_date(instrument['issue_date'], END_DATE)

    # Generate a realistic price based on instrument type
    if instrument['instrument_type'] in ['Stock', 'ETF', 'REIT']:
        base_price = random_decimal(10, 1000)
    elif instrument['instrument_type'] in ['Bond', 'CD']:
        base_price = random_decimal(90, 110)  # Around par value
    else:
        base_price = random_decimal(1, 500)

    # Generate other price components
    variation = random_decimal(0.95, 1.05)
    open_price = base_price * variation
    high_price = open_price * random_decimal(1, 1.05)
    low_price = open_price * random_decimal(0.95, 1)
    close_price = str(random_decimal(low_price, high_price))
    adjusted_close = float(close_price) * random_decimal(0.99, 1.01)

    # Generate volume based on instrument type
    if instrument['instrument_type'] in ['Stock', 'ETF']:
        volume = str(random.randint(10000, 10000000))
    else:
        volume = str(random.randint(1000, 1000000))

    # Generate market cap for equities
    if instrument['instrument_type'] in ['Stock', 'REIT']:
        market_cap = float(close_price) * random.randint(1000000, 1000000000)
    else:
        market_cap = None

    # Generate dividend and split information
    dividend = random_decimal(0, 2) if random.random() < 0.3 else 0
    split_ratio = random.choice([1.0, 2.0, 3.0, 1.5, 0.5]) if random.random() < 0.05 else 1.0

    timestamp = datetime_to_str(random_datetime(
        datetime.datetime.combine(price_date, datetime.time.min),
        datetime.datetime.combine(price_date, datetime.time.max)
    ))

    price = {
        'price_id': i,
        'instrument_id': instrument['instrument_id'],
        'price_date': price_date,
        'open_price': open_price,
        'high_price': high_price,
        'low_price': low_price,
        'close_price': close_price,
        'adjusted_close': adjusted_close,
        'volume': volume,
        'market_cap': market_cap,
        'dividend': dividend,
        'split_ratio': split_ratio,
        'data_source': random.choice(DATA_SOURCES),
        'timestamp': timestamp
    }
    instrument_prices.append(price)

print("Inserting InstrumentPrices data...")
for price in tqdm(instrument_prices):
    execute_query('''
    INSERT INTO InstrumentPrices (price_id, instrument_id, price_date, open_price, high_price, low_price, close_price, adjusted_close, volume, market_cap, dividend, split_ratio, data_source, timestamp)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        price['price_id'],
        price['instrument_id'],
        date_to_str(price['price_date']),
        price['open_price'],
        price['high_price'],
        price['low_price'],
        price['close_price'],
        price['adjusted_close'],
        price['volume'],
        price['market_cap'],
        price['dividend'],
        price['split_ratio'],
        price['data_source'],
        price['timestamp']
    ))
conn.commit()

# Generate InstrumentRatings data
instrument_ratings = []
rid = 1
while len(instrument_ratings) < NUM_INSTRUMENT_RATINGS:
    instrument = random.choice(investment_instruments)

    # Only rate bonds and stocks primarily
    if instrument['instrument_type'] not in ['Bond', 'Stock', 'ETF', 'Mutual Fund']:
        continue

    rating_date = random_date(instrument['issue_date'], END_DATE)
    rating_agency = random.choice(RATING_AGENCIES)

    # Different rating scales for different instrument types
    if instrument['instrument_type'] == 'Bond':
        rating_value = random.choice(RATINGS)
        previous_rating = random.choice(RATINGS)
    else:
        rating_value = f"{random.randint(1, 5)} Stars"
        previous_rating = f"{random.randint(1, 5)} Stars"

    outlook = random.choice(OUTLOOKS)

    # Determine if investment grade for bonds
    if instrument['instrument_type'] == 'Bond':
        is_investment_grade = 'Yes' if rating_value in ['AAA', 'AA+', 'AA', 'AA-', 'A+', 'A', 'A-', 'BBB+', 'BBB', 'BBB-'] else 'No'
    else:
        is_investment_grade = None

    rating = {
        'rating_id': rid,
        'instrument_id': instrument['instrument_id'],
        'rating_agency': rating_agency,
        'rating_value': rating_value,
        'rating_date': rating_date,
        'previous_rating': previous_rating,
        'outlook': outlook,
        'report_url': f"https://ratings.example.com/{rating_agency.lower().replace(' ', '')}/{instrument['ticker']}/{rating_date.strftime('%Y%m%d')}",
        'is_investment_grade': is_investment_grade
    }
    instrument_ratings.append(rating)
    rid += 1

print("Inserting InstrumentRatings data...")
for rating in tqdm(instrument_ratings):
    execute_query('''
    INSERT INTO InstrumentRatings (rating_id, instrument_id, rating_agency, rating_value, rating_date, previous_rating, outlook, report_url, is_investment_grade)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        rating['rating_id'],
        rating['instrument_id'],
        rating['rating_agency'],
        rating['rating_value'],
        date_to_str(rating['rating_date']),
        rating['previous_rating'],
        rating['outlook'],
        rating['report_url'],
        rating['is_investment_grade']
    ))
conn.commit()

def random_decimal(low, high, precision=1):
    return round(random.uniform(low, high), precision)

# Helper to get a random date

def random_date(start, end):
    delta = end - start
    return start + datetime.timedelta(days=random.randint(0, delta.days))

# Helper to get a random datetime

def random_datetime(start, end):
    delta = end - start
    return start + datetime.timedelta(seconds=random.randint(0, int(delta.total_seconds())))

NUM_ECONOMIC_INDICATOR_VALUES = 1000
END_DATE = datetime.date.today()

economic_indicator_values = []
for i in range(1, NUM_ECONOMIC_INDICATOR_VALUES + 1):
    indicator = random.choice(economic_indicators)

    # Generate dates based on frequency
    frequency = indicator['frequency']
    if frequency == 'Daily':
        date = random_date(datetime.date(2015, 1, 1), END_DATE)
    elif frequency == 'Weekly':
        date = random_date(datetime.date(2015, 1, 1), END_DATE)
        while date.weekday() != 0:
            date -= datetime.timedelta(days=1)
    elif frequency == 'Monthly':
        year = random.randint(2015, END_DATE.year)
        month = random.randint(1, 12)
        date = datetime.date(year, month, 1)
    elif frequency == 'Quarterly':
        year = random.randint(2015, END_DATE.year)
        quarter = random.randint(1, 4)
        month = (quarter - 1) * 3 + 1
        date = datetime.date(year, month, 1)
    else:
        year = random.randint(2015, END_DATE.year)
        month = 1 if frequency == 'Annually' else random.choice([1, 7])
        date = datetime.date(year, month, 1)

    # Generate realistic values
    if indicator['indicator_type'] in ['GDP', 'Retail Sales', 'Industrial Production']:
        value = str(random_decimal(-5, 10, 1))
    elif indicator['indicator_type'] in ['Inflation', 'Unemployment', 'Interest Rate']:
        value = str(random_decimal(0, 10, 1))
    elif indicator['indicator_type'] in ['Consumer Confidence', 'PMI']:
        value = str(random_decimal(30, 120, 1))
    elif indicator['indicator_type'] == 'Housing Starts':
        value = str(random.randint(500000, 2000000))
    else:
        value = str(random_decimal(-50, 100, 1))

    # Generate related values
    previous_value = float(value) * random_decimal(0.9, 1.1)
    # Guard against zero denominator
    if abs(previous_value) < 1e-8:
        percentage_change = '0.0'
    else:
        pct = (float(value) - previous_value) / abs(previous_value) * 100
        percentage_change = str(round(pct, 2))
    forecast_value = float(value) * random_decimal(0.95, 1.05)

    # Generate release dates
    actual_release_date = random_datetime(
        datetime.datetime.combine(date, datetime.time.min),
        datetime.datetime.combine(date + datetime.timedelta(days=30), datetime.time.max)
    )
    next_release_date = date + datetime.timedelta(days=30)

    indicator_value = {
        'value_id': i,
        'indicator_id': indicator['indicator_id'],
        'date': date,
        'value': value,
        'previous_value': previous_value,
        'percentage_change': percentage_change,
        'forecast_value': forecast_value,
        'actual_release_date': actual_release_date,
        'next_release_date': next_release_date
    }
    economic_indicator_values.append(indicator_value)

print("Inserting EconomicIndicatorValues data...")
for iv in tqdm(economic_indicator_values):
    execute_query('''
    INSERT INTO EconomicIndicatorValues \
        (value_id, indicator_id, date, value, previous_value, percentage_change, forecast_value, actual_release_date, next_release_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        iv['value_id'],
        iv['indicator_id'],
        date_to_str(iv['date']),
        iv['value'],
        iv['previous_value'],
        iv['percentage_change'],
        iv['forecast_value'],
        datetime_to_str(iv['actual_release_date']),
        date_to_str(iv['next_release_date'])
    ))
conn.commit()

# Generate ExchangeRates data
exchange_rates = []
currency_pairs = []
for from_currency in CURRENCIES:
    for to_currency in CURRENCIES:
        if from_currency != to_currency:
            currency_pairs.append((from_currency, to_currency))

# Select random pairs to match the number needed
selected_pairs = random.sample(currency_pairs, min(len(currency_pairs), NUM_EXCHANGE_RATES))
remaining_count = NUM_EXCHANGE_RATES - len(selected_pairs)

# Fill remaining with repeated pairs but different dates if needed
if remaining_count > 0:
    additional_pairs = random.choices(currency_pairs, k=remaining_count)
    selected_pairs.extend(additional_pairs)

for i in range(1, NUM_EXCHANGE_RATES + 1):
    from_currency, to_currency = selected_pairs[i-1]
    rate_date = random_date(START_DATE, END_DATE)

    # Generate realistic exchange rates based on common currency relationships
    if from_currency == 'USD':
        if to_currency == 'EUR':
            exchange_rate = random_decimal(0.8, 1.0, 6)
        elif to_currency == 'GBP':
            exchange_rate = random_decimal(0.7, 0.85, 6)
        elif to_currency == 'JPY':
            exchange_rate = random_decimal(100, 150, 6)
        elif to_currency in ['AUD', 'CAD']:
            exchange_rate = random_decimal(1.2, 1.5, 6)
        else:
            exchange_rate = random_decimal(0.5, 100, 6)
    else:
        exchange_rate = random_decimal(0.5, 100, 6)

    inverse_rate = str(round(1 / exchange_rate, 6))
    data_source = random.choice(DATA_SOURCES)

    exchange_rates.append({
        'rate_id': i,
        'from_currency': from_currency,
        'to_currency': to_currency,
        'rate_date': rate_date,
        'exchange_rate': exchange_rate,
        'inverse_rate': inverse_rate,
        'data_source': data_source
    })

print("Inserting ExchangeRates data...")
for rate in tqdm(exchange_rates):
    execute_query('''
    INSERT INTO ExchangeRates (rate_id, from_currency, to_currency, rate_date, exchange_rate, inverse_rate, data_source)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        rate['rate_id'],
        rate['from_currency'],
        rate['to_currency'],
        date_to_str(rate['rate_date']),
        rate['exchange_rate'],
        rate['inverse_rate'],
        rate['data_source']
    ))
conn.commit()

# Generate Portfolios data
portfolios = []
for i in range(1, NUM_PORTFOLIOS + 1):
    portfolio_id = generate_id('PF', 10)
    portfolio_name = fake.word().capitalize() + ' ' + random.choice(['Portfolio', 'Fund', 'Strategy', 'Allocation'])
    customer_id = random.choice(customers)['customer_id']
    advisor_id  = random.choice(advisors)['advisor_id']
    inception_date = random_date(datetime.date(2015, 1, 1), END_DATE)
    portfolio_type = random.choice(PORTFOLIO_TYPES)
    benchmark_id = generate_id('BM', 10)
    management_style = random.choice(MANAGEMENT_STYLES)
    risk_tolerance = random.choice(RISK_TOLERANCES)
    target_return = str(random_decimal(1, 15, 2)) + '%'
    status = random.choice(STATUSES)
    last_rebalance_date = random_date(inception_date, END_DATE)

    portfolios.append({
        'portfolio_id': portfolio_id,
        'portfolio_name': portfolio_name,
        'customer_id': customer_id,
        'advisor_id': advisor_id,
        'inception_date': inception_date,
        'portfolio_type': portfolio_type,
        'benchmark_id': benchmark_id,
        'management_style': management_style,
        'risk_tolerance': risk_tolerance,
        'target_return': target_return,
        'status': status,
        'last_rebalance_date': last_rebalance_date
    })

print("Inserting Portfolios data...")
for portfolio in tqdm(portfolios):
    execute_query('''
    INSERT INTO Portfolios (portfolio_id, portfolio_name, customer_id, advisor_id, inception_date, portfolio_type,
    benchmark_id, management_style, risk_tolerance, target_return, status, last_rebalance_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        portfolio['portfolio_id'],
        portfolio['portfolio_name'],
        portfolio['customer_id'],
        portfolio['advisor_id'],
        date_to_str(portfolio['inception_date']),
        portfolio['portfolio_type'],
        portfolio['benchmark_id'],
        portfolio['management_style'],
        portfolio['risk_tolerance'],
        portfolio['target_return'],
        portfolio['status'],
        date_to_str(portfolio['last_rebalance_date'])
    ))
conn.commit()

# Generate PortfolioHoldings data
portfolio_holdings = []
portfolio_ids = [p['portfolio_id'] for p in portfolios]
instrument_ids = [generate_id('INS', 10) for _ in range(NUM_INVESTMENT_INSTRUMENTS)]

for i in range(1, NUM_PORTFOLIO_HOLDINGS + 1):
    portfolio_id = random.choice(portfolio_ids)
    instrument_id = random.choice(instrument_ids)
    portfolio = next((p for p in portfolios if p['portfolio_id'] == portfolio_id), None)
    purchase_date = random_date(portfolio['inception_date'], END_DATE)

    purchase_price = random_decimal(10, 1000, 4)
    current_price = purchase_price * random_decimal(0.8, 1.2, 4)  # Current price near purchase price
    quantity = random_decimal(1, 10000, 4)
    market_value = str(round(quantity * current_price, 2))
    weight = random_decimal(0, 0.25, 4)  # Maximum 25% weight for diversification
    currency = random.choice(CURRENCIES)
    notes = fake.sentence() if random.random() < 0.3 else None
    last_updated = random_datetime(datetime.datetime.combine(purchase_date, datetime.time.min),
                                  datetime.datetime.combine(END_DATE, datetime.time.max))

    portfolio_holdings.append({
        'holding_id': i,
        'portfolio_id': portfolio_id,
        'instrument_id': instrument_id,
        'purchase_date': purchase_date,
        'quantity': quantity,
        'purchase_price': purchase_price,
        'current_price': current_price,
        'market_value': market_value,
        'weight': weight,
        'currency': currency,
        'notes': notes,
        'last_updated': last_updated
    })

print("Inserting PortfolioHoldings data...")
for holding in tqdm(portfolio_holdings):
    execute_query('''
    INSERT INTO PortfolioHoldings (holding_id, portfolio_id, instrument_id, purchase_date, quantity, purchase_price,
    current_price, market_value, weight, currency, notes, last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        holding['holding_id'],
        holding['portfolio_id'],
        holding['instrument_id'],
        date_to_str(holding['purchase_date']),
        holding['quantity'],
        holding['purchase_price'],
        holding['current_price'],
        holding['market_value'],
        holding['weight'],
        holding['currency'],
        holding['notes'],
        datetime_to_str(holding['last_updated'])
    ))
conn.commit()

# Generate PortfolioTransactions data
portfolio_transactions = []
for i in range(1, NUM_PORTFOLIO_TRANSACTIONS + 1):
    transaction_id = generate_id('TXN', 15)
    portfolio_id = random.choice(portfolio_ids)
    instrument_id = random.choice(instrument_ids)

    portfolio = next((p for p in portfolios if p['portfolio_id'] == portfolio_id), None)
    transaction_date_dt = random_datetime(
        datetime.datetime.combine(portfolio['inception_date'], datetime.time.min),
        datetime.datetime.combine(END_DATE, datetime.time.max)
    )
    transaction_date = datetime_to_str(transaction_date_dt)

    transaction_type = random.choice(TRANSACTION_TYPES)
    quantity = random_decimal(1, 10000, 4)
    price = random_decimal(10, 1000, 4)
    amount = str(round(quantity * price, 2))
    fees = random_decimal(0, 50, 2)
    taxes = random_decimal(0, 100, 2) if transaction_type == 'Sell' else 0
    currency = random.choice(CURRENCIES)

    # Settlement typically happens T+2 for most securities
    settlement_date = transaction_date_dt.date() + datetime.timedelta(days=2)
    broker_id = generate_id('BRK', 10)
    notes = fake.sentence() if random.random() < 0.3 else None

    portfolio_transactions.append({
        'transaction_id': transaction_id,
        'portfolio_id': portfolio_id,
        'instrument_id': instrument_id,
        'transaction_date': transaction_date,
        'transaction_type': transaction_type,
        'quantity': quantity,
        'price': price,
        'amount': amount,
        'fees': fees,
        'taxes': taxes,
        'currency': currency,
        'settlement_date': settlement_date,
        'broker_id': broker_id,
        'notes': notes
    })

print("Inserting PortfolioTransactions data...")
for transaction in tqdm(portfolio_transactions):
    execute_query('''
    INSERT INTO PortfolioTransactions (transaction_id, portfolio_id, instrument_id, transaction_date, transaction_type,
    quantity, price, amount, fees, taxes, currency, settlement_date, broker_id, notes)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        transaction['transaction_id'],
        transaction['portfolio_id'],
        transaction['instrument_id'],
        transaction['transaction_date'],
        transaction['transaction_type'],
        transaction['quantity'],
        transaction['price'],
        transaction['amount'],
        transaction['fees'],
        transaction['taxes'],
        transaction['currency'],
        date_to_str(transaction['settlement_date']),
        transaction['broker_id'],
        transaction['notes']
    ))
conn.commit()

# Generate PortfolioPerformance data
portfolio_performance = []
for i in range(1, NUM_PORTFOLIO_PERFORMANCE + 1):
    portfolio_id = random.choice(portfolio_ids)
    portfolio = next((p for p in portfolios if p['portfolio_id'] == portfolio_id), None)

    # Generate realistic date sequence
    perf_date = random_date(portfolio['inception_date'], END_DATE)

    market_value = random_decimal(100000, 10000000, 2)
    deposits = random_decimal(0, 100000, 2) if random.random() < 0.1 else 0
    withdrawals = random_decimal(0, 50000, 2) if random.random() < 0.05 else 0

    daily_return_percentage = random_decimal(-0.03, 0.03, 6)
    daily_return = str(round(market_value * daily_return_percentage / 100, 2))
    cumulative_return = random_decimal(-0.2, 0.5, 6)
    benchmark_return = random_decimal(-0.2, 0.5, 6)

    alpha = str(round(daily_return_percentage - benchmark_return, 4))
    beta = str(round(random_decimal(0.8, 1.2, 2), 2))
    sharpe_ratio = str(round(random_decimal(-0.5, 3.0, 2), 2))
    volatility = random_decimal(0.01, 0.2, 6)

    portfolio_performance.append({
        'performance_id': i,
        'portfolio_id': portfolio_id,
        'date': perf_date,
        'market_value': market_value,
        'deposits': deposits,
        'withdrawals': withdrawals,
        'daily_return': daily_return,
        'daily_return_percentage': daily_return_percentage,
        'cumulative_return': cumulative_return,
        'benchmark_return': benchmark_return,
        'alpha': alpha,
        'beta': beta,
        'sharpe_ratio': sharpe_ratio,
        'volatility': volatility
    })

print("Inserting PortfolioPerformance data...")
for performance in tqdm(portfolio_performance):
    execute_query('''
    INSERT INTO PortfolioPerformance (performance_id, portfolio_id, date, market_value, deposits, withdrawals,
    daily_return, daily_return_percentage, cumulative_return, benchmark_return, alpha, beta, sharpe_ratio, volatility)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        performance['performance_id'],
        performance['portfolio_id'],
        date_to_str(performance['date']),
        performance['market_value'],
        performance['deposits'],
        performance['withdrawals'],
        performance['daily_return'],
        performance['daily_return_percentage'],
        performance['cumulative_return'],
        performance['benchmark_return'],
        performance['alpha'],
        performance['beta'],
        performance['sharpe_ratio'],
        performance['volatility']
    ))
conn.commit()

# Close connection
conn.close()

print(f"Database successfully generated: {DB_FILE}")
print(f"Total tables: {22}")
print(f"Total records generated:")
print(f"  Regions: {NUM_REGIONS}")
print(f"  Countries: {NUM_COUNTRIES}")
print(f"  Sectors: {NUM_SECTORS}")
print(f"  Industries: {NUM_INDUSTRIES}")
print(f"  Branches: {NUM_BRANCHES}")
print(f"  Advisors: {NUM_ADVISORS}")
print(f"  Teams: {NUM_TEAMS}")
print(f"  TeamMembers: {NUM_TEAM_MEMBERS}")
print(f"  Markets: {NUM_MARKETS}")
print(f"  EconomicIndicators: {NUM_ECONOMIC_INDICATORS}")
print(f"  EconomicIndicatorValues: {NUM_ECONOMIC_INDICATOR_VALUES}")
print(f"  ExchangeRates: {NUM_EXCHANGE_RATES}")
print(f"  Customers: {NUM_CUSTOMERS}")
print(f"  CustomerSegments: {NUM_CUSTOMER_SEGMENTS}")
print(f"  ClientContacts: {NUM_CLIENT_CONTACTS}")
print(f"  InvestmentInstruments: {NUM_INVESTMENT_INSTRUMENTS}")
print(f"  InstrumentPrices: {NUM_INSTRUMENT_PRICES}")
print(f"  InstrumentRatings: {NUM_INSTRUMENT_RATINGS}")
print(f"  Portfolios: {NUM_PORTFOLIOS}")
print(f"  PortfolioHoldings: {NUM_PORTFOLIO_HOLDINGS}")
print(f"  PortfolioTransactions: {NUM_PORTFOLIO_TRANSACTIONS}")
print(f"  PortfolioPerformance: {NUM_PORTFOLIO_PERFORMANCE}")


