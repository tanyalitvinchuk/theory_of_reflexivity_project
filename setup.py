import mysql.connector

def read_secrets(filename=".secrets"):
    """Read database configuration from a .secrets file."""
    params = {}
    with open(filename, 'r') as file:
        for line in file:
            key, value = line.strip().split('=')
            params[key] = value
    return params

def create_database_connection():
    # Read database parameters from the .secrets file
    config = read_secrets()
    mydb = mysql.connector.connect(
        host=config['host'],
        user=config['user'],
        password=config['password'],
        database=config['database']
    )
    return mydb

def create_tables(mydb):
    mycursor = mydb.cursor()
    mycursor.execute("""
        CREATE TABLE IF NOT EXISTS stockData (
            symbol VARCHAR(255) PRIMARY KEY,
            shortName VARCHAR(255),
            country VARCHAR(255),
            industry VARCHAR(255),
            sector VARCHAR(255),
            previousClose DECIMAL(10,4),
            beta DECIMAL(10,4),
            trailingPE DECIMAL(10,4),
            forwardPE DECIMAL(10,4),
            volume BIGINT,
            averageVolume BIGINT,
            averageVolume10days BIGINT,
            marketCap BIGINT,
            fiftyTwoWeekLow DECIMAL(10,4),
            fiftyTwoWeekHigh DECIMAL(10,4),
            fiftyDayAverage DECIMAL(10,4),
            twoHundredDayAverage DECIMAL(10,4),
            bookValue DECIMAL(10,4),
            priceToBook DECIMAL(10,4),
            52WeekChange DECIMAL(10,4),
            percentDifferenceFrom52WeekLow DECIMAL(10,4),
            percentDifferenceFrom52WeekHigh DECIMAL(10,4),
            spGroup VARCHAR(255)
        )
    """)
    mycursor.execute("""
        CREATE TABLE IF NOT EXISTS stockPrices (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE,
            symbol VARCHAR(255),
            open DECIMAL(10,4),
            high DECIMAL(10,4),
            low DECIMAL(10,4),
            close DECIMAL(10,4),
            adjustedClose DECIMAL(10,4),
            volume BIGINT,
            fiftyTwoWeekLow DECIMAL(10,4),
            fiftyTwoWeekHigh DECIMAL(10,4),
            percentDifferenceFrom52WeekLow DECIMAL(10,4),
            percentDifferenceFrom52WeekHigh DECIMAL(10,4)
        )
    """)
    mydb.commit()

if __name__ == "__main__":
    db = create_database_connection()
    create_tables(db)
