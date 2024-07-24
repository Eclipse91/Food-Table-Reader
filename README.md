# Food Components Data Extractor

## Overview
This script is designed to extract nutritional data from a given URL, process the data to convert measurements into milligrams (mg) where necessary, and save the data to a SQLite database as well as CSV files.

## Features
1. **Web Scraping**: Uses Selenium WebDriver to scrape data from web pages.
2. **Data Processing**: Converts nutritional values to a consistent unit (mg).
3. **Database Storage**: Saves the processed data into a SQLite database.
4. **CSV Export**: Exports the data to CSV files for easy access and analysis.

## Prerequisites

1. **Python 3.x**
2. **Selenium**:
   ```bash
   pip install selenium
   ```
3. **SQLAlchemy**:
   ```bash
   pip install SQLAlchemy
   ```
4. **Pandas**:
   ```bash
   pip install pandas
   ```
5. **WebDriver for Selenium**: Ensure you have the appropriate WebDriver for your browser installed and added to your PATH. For example, [ChromeDriver](https://sites.google.com/a/chromium.org/chromedriver/).

## Files
- `urls.txt`: A text file containing the URLs to scrape, one per line.
- `components.db`: The SQLite database file where data will be stored.

## Script Breakdown

### `convert_to_mg(data)`
Converts nutritional values to milligrams (mg).
- **Input**: List of data with values and units.
- **Output**: List of data converted to mg.

### `list_to_dict(data)`
Converts a list of lists into a dictionary.
- **Input**: List of lists.
- **Output**: Dictionary.

### `save_to_db(df, table_name, db_path='sqlite:///components.db')`
Saves a Pandas DataFrame to the SQLite database.
- **Input**: DataFrame, table name, database path.
- **Output**: None.

### `extract_table_data(url)`
Extracts table data from a given URL using Selenium, processes the data, and saves it to both the database and CSV files.
- **Input**: URL of the web page.
- **Output**: DataFrame, CSV file path.

### `read_urls_file(file_path)`
Reads a file containing URLs.
- **Input**: File path.
- **Output**: List of URLs.

## Usage
1. **Prepare the URL List**: Create a file named `urls.txt` and add the URLs you want to scrape, one per line.
2. **Run the Script**:
   ```bash
   python main.py
   ```

## Example
1. **Create `urls.txt`**:
   ```
   https://fdc.nal.usda.gov/fdc-app.html#/food-details/2346393/nutrients
   https://fdc.nal.usda.gov/fdc-app.html#/food-details/2346392/nutrients
   ```
2. **Run the Script**:
   ```bash
   python main.py
   ```
3. **Output**:
   - CSV files saved in the `foods/` directory.
   - Data stored in `components.db`.

## Error Handling
- If an error occurs during data extraction, an empty DataFrame is returned, and the error is printed to the console.

## Notes
- Ensure the WebDriver executable is in your PATH.
- Customize the database path and table names as needed.
- Modify the script to handle different table structures if necessary.