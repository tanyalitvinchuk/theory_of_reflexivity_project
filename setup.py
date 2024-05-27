import mysql.connector

def create_database_connection():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="your_password",  # Update with your actual password
        database="your_database"  # Update with your actual database name
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
