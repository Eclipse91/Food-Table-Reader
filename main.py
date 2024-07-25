import os
import logging
from time import time
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, exc, inspect, text
from sqlalchemy.exc import SQLAlchemyError

def convert_to_mg(data):
    # Convert data to mg if necessary
    processed_data = []
    for mineral, value, unit in data:
        if '<' in value:
            numeric_value = float(value.replace('<', ''))
            if unit == 'µg':
                value_mg = f"<{numeric_value / 1000}"  # Convert µg to mg
            elif unit == 'g':
                value_mg = f"<{numeric_value * 1000}"  # Convert g to mg
            else:
                value_mg = f"<{numeric_value}"
        else:
            numeric_value = float(value)
            if unit == 'µg':
                value_mg = numeric_value / 1000  # Convert µg to mg
            elif unit == 'g':
                value_mg = numeric_value * 1000  # Convert g to mg
            else:
                value_mg = numeric_value
        processed_data.append([mineral, value_mg])
    
    return processed_data

def list_to_dict(data):
    food_dict = {}
    for item in data:
        key = item[0]
        value = item[1]
        food_dict[key] = value
    return food_dict
    
def save_to_db(df, table_name, db_path='sqlite:///components.db'):
    print("Starting save_to_db function")
    engine = create_engine(db_path)
    meta = MetaData()
    inspector = inspect(engine)

    # Define initial columns (assuming 'Food' as the primary key)
    initial_columns = [Column('Food', String, primary_key=True)]
    for col in df.columns:
        if col != 'Food':
            initial_columns.append(Column(col, String))

    print("Initial columns for table:", initial_columns)

    with engine.connect() as connection:
        # Check if the table exists
        if not connection.dialect.has_table(connection, table_name):
            # Define the table schema
            table = Table(table_name, meta, *initial_columns, extend_existing=True)
            # Create table in database if it doesn't exist
            try:
                meta.create_all(engine)
                print("Table created successfully")
            except exc.SQLAlchemyError as e:
                logging.error(f"Error creating table: {e}")
                # print(f"Error creating table: {e}")
        else:
            print("Table already exists")
            # Get existing columns
            existing_columns = inspector.get_columns(table_name)
            existing_column_names = [col['name'] for col in existing_columns]
            print("Existing columns:", existing_column_names)

            # Find new columns to add
            new_columns = []
            for col in df.columns:
                if col not in existing_column_names:
                    new_columns.append(Column(col, String))
            
            print("New columns to add:", new_columns)

            # Add new columns if any
            if new_columns:
                for column in new_columns:
                    alter_stmt = text(f'ALTER TABLE {table_name} ADD COLUMN "{column.name}" {column.type}')
                    try:
                        connection.execute(alter_stmt)
                        print(f"Added column {column.name} to table {table_name}")
                    except exc.SQLAlchemyError as e:
                        logging.error(f"Error adding column {column.name}: {e}")
                        # print(f"Error adding column {column.name}: {e}")

                # Update existing rows with default values for new columns
                for column in new_columns:
                    update_stmt = text(f'UPDATE {table_name} SET "{column.name}" = "0"')
                    try:
                        connection.execute(update_stmt)
                        print(f"Updated existing rows with default value for column {column.name}")
                    except exc.SQLAlchemyError as e:
                        logging.error(f"Error updating existing rows for column {column.name}: {e}")
                        # print(f"Error updating existing rows for column {column.name}: {e}")

        # Insert or update the records
        table = Table(table_name, meta, autoload_with=engine)

    # Insert or update the record
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            for _, row in df.iterrows():
                data = row.to_dict()
                # Convert all data to string
                data = {key: str(value) for key, value in data.items()}
                print("Data to insert:", data)
                stmt = table.insert().values(data).prefix_with("OR REPLACE")
                print("SQL Statement:", str(stmt))
                connection.execute(stmt)
            transaction.commit()
            print("Data inserted successfully")
        except SQLAlchemyError as e:
            transaction.rollback()
            logging.error(f"Error inserting data: {e}")
            # print(f"Error inserting data: {e}")
    print("Completed save_to_db function")

def extract_table_data(driver, url):
    try:
        # Navigate to the URL
        driver.get(url)

        # Wait for the table header to be present
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.XPATH, "//thead//th")))

        # Path of the file
        food = driver.find_element(By.ID, "foodDetailsDescription").text

        # Locate the table header
        headers = driver.find_elements(By.XPATH, "//thead//th")

        # Extract the headers (only first 3 headers)
        header_list = [header.text.strip() for header in headers[:3]]

        # Locate the table rows
        rows = driver.find_elements(By.XPATH, "//tbody//tr")

        proximates = []
        carbohydrates = []
        minerals = []
        vitamins = []
        lipids = []
        amino_acids = []
        phytosterols = []
        organic_acids = []
        
        # Extract the data from the rows
        table_data = []
        full_table_data = []
        for row in rows:
            cells = row.find_elements(By.XPATH, ".//td")
            cell_data = [cell.text.strip() for cell in cells[:3]]  # Only take the first 3 cells
            
            # If the number of cells is less than 3, pad with None
            if len(cell_data) < 3:
                cell_data.extend([None] * (3 - len(cell_data)))
            if cell_data != [None, None, None] and cell_data != ['','','']:
                full_table_data.append(cell_data)
                table_data.append(cell_data)
                match table_data[0][0]:
                    case 'Proximates:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:']:
                            proximates.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Carbohydrates:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:']:
                            carbohydrates.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Minerals:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:']:
                            minerals.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Vitamins and Other Components:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:']:
                            if '' in cell_data:
                                continue
                            vitamins.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Lipids:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:']:
                            lipids.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Amino acids:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:']:
                            amino_acids.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Phytosterols:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:']:
                            phytosterols.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Organic acids:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:']:
                            organic_acids.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)                           

        if proximates:
            proximates = convert_to_mg(proximates)
            proximates.insert(0, ['Food', food])
            proximates_dict = list_to_dict(proximates)
            dfproximates = pd.DataFrame([proximates_dict])
            save_to_db(dfproximates, 'proximates')

        if carbohydrates:
            carbohydrates = convert_to_mg(carbohydrates)
            carbohydrates.insert(0, ['Food', food])
            carbohydrates = list_to_dict(carbohydrates)
            dfcarbohydrates = pd.DataFrame([carbohydrates])
            save_to_db(dfcarbohydrates, 'carbohydrates')

        if minerals:
            minerals = convert_to_mg(minerals)
            minerals.insert(0, ['Food', food])
            minerals = list_to_dict(minerals)
            dfminerals = pd.DataFrame([minerals])
            save_to_db(dfminerals, 'minerals')

        if vitamins:
            vitamins = convert_to_mg(vitamins)
            vitamins.insert(0, ['Food', food])
            vitamins = list_to_dict(vitamins)
            dfvitamins = pd.DataFrame([vitamins])
            save_to_db(dfvitamins, 'vitamins')

        if lipids:
            lipids = convert_to_mg(lipids)
            lipids.insert(0, ['Food', food])
            lipids = list_to_dict(lipids)
            dflipids = pd.DataFrame([lipids])
            save_to_db(dflipids, 'lipids')

        if amino_acids:
            amino_acids = convert_to_mg(amino_acids)
            amino_acids.insert(0, ['Food', food])
            amino_acids = list_to_dict(amino_acids)
            dfamino_acids = pd.DataFrame([amino_acids])
            save_to_db(dfamino_acids, 'amino_acids')

        if phytosterols:
            phytosterols = convert_to_mg(phytosterols)
            phytosterols.insert(0, ['Food', food])
            phytosterols = list_to_dict(phytosterols)
            dfphytosterols = pd.DataFrame([phytosterols])
            save_to_db(dfphytosterols, 'phytosterols')

        if organic_acids:
            organic_acids = convert_to_mg(organic_acids)
            organic_acids.insert(0, ['Food', food])
            organic_acids = list_to_dict(organic_acids)
            dforganic_acids = pd.DataFrame([organic_acids])
            save_to_db(dforganic_acids, 'organic_acids')

        df = pd.DataFrame(full_table_data, columns=header_list)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        df = pd.DataFrame()  # Return an empty DataFrame on error

    return df, 'foods/' + food + '.csv'

def read_urls_file(file_path):
    with open(file_path, 'r') as file:
        urls = [line.strip() for line in file.readlines()]

    return urls    

def log_configurator():
    '''
    Configure and initialize the logger.
    '''
    log_directory = './logs/'
    os.makedirs(log_directory, exist_ok=True)
    current_datetime = datetime.now()
    current_file_name = os.path.splitext(os.path.basename(__file__))[0]
    formatted_datetime = current_datetime.strftime('%Y%m%d_%H%M%S')
    log_file = f'{log_directory}{current_file_name}_{formatted_datetime}.log'

    logging.basicConfig(
        filename=log_file, level=logging.INFO, format='%(message)s'
        )
    logging.info('Program started')

def main():
    # Configure and initialize the logger file
    log_configurator()
    
    urls = read_urls_file('urls.txt')

    # Set up the Chrome WebDriver (Ensure the driver executable is in your PATH or specify the path explicitly)
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Run in headless mode (no GUI)

    for url in urls:
        try:
            # Initialize the WebDriver
            driver = webdriver.Chrome(service=service, options=options)

            df, output_csv_path = extract_table_data(driver, url)

            # Save the DataFrame to a CSV file
            df.to_csv(output_csv_path, index=False)
            print(f"Data successfully saved to {output_csv_path}")

            driver.quit()
        except:
            logging.error('error in the driver')
        finally:
            # Ensure the WebDriver is properly closed
            driver.quit()

if __name__ == '__main__':
    main()