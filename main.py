import os
import logging
from time import sleep
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.service import Service
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, exc, inspect, text
from sqlalchemy.exc import SQLAlchemyError

URLS = '' # 'urls_20240726_103810.txt'
CORRECTED_FOODS = '' # 'corrected_foods_20240726_150227.txt'

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
    
def save_to_db(df, table_name, db_path='sqlite:///food_components.db'):
    logging.info("Starting save_to_db function")
    engine = create_engine(db_path)
    meta = MetaData()
    inspector = inspect(engine)

    # Define initial columns (assuming 'Food' as the primary key)
    initial_columns = [Column('Food', String, primary_key=True)]
    for col in df.columns:
        if col != 'Food':
            initial_columns.append(Column(col, String))

    logging.info("Initial columns for table:", initial_columns)

    with engine.connect() as connection:
        # Check if the table exists
        if not connection.dialect.has_table(connection, table_name):
            # Define the table schema
            table = Table(table_name, meta, *initial_columns, extend_existing=True)
            # Create table in database if it doesn't exist
            try:
                meta.create_all(engine)
                logging.info("Table created successfully")
            except exc.SQLAlchemyError as e:
                logging.error(f"Error creating table: {e}")
        else:
            logging.info("Table already exists")
            # Get existing columns
            existing_columns = inspector.get_columns(table_name)
            existing_column_names = [col['name'] for col in existing_columns]
            logging.info("Existing columns:", existing_column_names)

            # Find new columns to add
            new_columns = []
            for col in df.columns:
                if col not in existing_column_names:
                    new_columns.append(Column(col, String))
            
            logging.info("New columns to add:", new_columns)

            # Add new columns if any
            if new_columns:
                for column in new_columns:
                    alter_stmt = text(f'ALTER TABLE {table_name} ADD COLUMN "{column.name}" {column.type}')
                    try:
                        connection.execute(alter_stmt)
                        logging.info(f"Added column {column.name} to table {table_name}")
                    except exc.SQLAlchemyError as e:
                        logging.error(f"Error adding column {column.name}: {e}")

                # Update existing rows with default values for new columns
                for column in new_columns:
                    update_stmt = text(f'UPDATE {table_name} SET "{column.name}" = "0"')
                    try:
                        connection.execute(update_stmt)
                        logging.info(f"Updated existing rows with default value for column {column.name}")
                    except exc.SQLAlchemyError as e:
                        logging.error(f"Error updating existing rows for column {column.name}: {e}")

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
                logging.info("Data to insert:", data)
                stmt = table.insert().values(data).prefix_with("OR REPLACE")
                logging.info("SQL Statement:", str(stmt))
                connection.execute(stmt)
            transaction.commit()
            logging.info("Data inserted successfully")
        except SQLAlchemyError as e:
            transaction.rollback()
            logging.error(f"Error inserting data: {e}")
    logging.info("Completed save_to_db function")

def extract_table_data(driver, url, folder_name):
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
        isoflavones = []
        oligosaccharides = []
        
        # Extract the data from the rows
        table_data = []
        full_table_data = []
        for row in rows:
            cells = row.find_elements(By.XPATH, ".//td")
            cell_data = [cell.text.strip() for cell in cells[:3]]  # Only take the first 3 cells
            'https://fdc.nal.usda.gov/fdc-app.html#/food-details/2262074/nutrients'
            # If the number of cells is less than 3, pad with None
            if len(cell_data) < 3:
                cell_data.extend([None] * (3 - len(cell_data)))
            if cell_data != [None, None, None] and cell_data != ['','','']:
                full_table_data.append(cell_data)
                table_data.append(cell_data)
                match table_data[0][0]:
                    case 'Proximates:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            proximates.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Carbohydrates:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            carbohydrates.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Minerals:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            minerals.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Vitamins and Other Components:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            if '' in cell_data:
                                continue
                            vitamins.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Lipids:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            lipids.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Amino acids:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            amino_acids.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Phytosterols:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            phytosterols.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Organic acids:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            organic_acids.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Isoflavones:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            isoflavones.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Oligosaccharides:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:', 'Phytosterols:', 'Organic acids:', 'Isoflavones:', 'Oligosaccharides:']:
                            oligosaccharides.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)                     

        if proximates:
            proximates = convert_to_mg(proximates)
            proximates.insert(0, ['Food', food])
            proximates_dict = list_to_dict(proximates)
            dfproximates = pd.DataFrame([proximates_dict])
            save_to_db(dfproximates, 'proximates', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        if carbohydrates:
            carbohydrates = convert_to_mg(carbohydrates)
            carbohydrates.insert(0, ['Food', food])
            carbohydrates = list_to_dict(carbohydrates)
            dfcarbohydrates = pd.DataFrame([carbohydrates])
            save_to_db(dfcarbohydrates, 'carbohydrates', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        if minerals:
            minerals = convert_to_mg(minerals)
            minerals.insert(0, ['Food', food])
            minerals = list_to_dict(minerals)
            dfminerals = pd.DataFrame([minerals])
            save_to_db(dfminerals, 'minerals', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        if vitamins:
            vitamins = convert_to_mg(vitamins)
            vitamins.insert(0, ['Food', food])
            vitamins = list_to_dict(vitamins)
            dfvitamins = pd.DataFrame([vitamins])
            save_to_db(dfvitamins, 'vitamins', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        if lipids:
            lipids = convert_to_mg(lipids)
            lipids.insert(0, ['Food', food])
            lipids = list_to_dict(lipids)
            dflipids = pd.DataFrame([lipids])
            save_to_db(dflipids, 'lipids', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        if amino_acids:
            amino_acids = convert_to_mg(amino_acids)
            amino_acids.insert(0, ['Food', food])
            amino_acids = list_to_dict(amino_acids)
            dfamino_acids = pd.DataFrame([amino_acids])
            save_to_db(dfamino_acids, 'amino_acids', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        if phytosterols:
            phytosterols = convert_to_mg(phytosterols)
            phytosterols.insert(0, ['Food', food])
            phytosterols = list_to_dict(phytosterols)
            dfphytosterols = pd.DataFrame([phytosterols])
            save_to_db(dfphytosterols, 'phytosterols', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        if organic_acids:
            organic_acids = convert_to_mg(organic_acids)
            organic_acids.insert(0, ['Food', food])
            organic_acids = list_to_dict(organic_acids)
            dforganic_acids = pd.DataFrame([organic_acids])
            save_to_db(dforganic_acids, 'organic_acids', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        if isoflavones:
            isoflavones = convert_to_mg(isoflavones)
            isoflavones.insert(0, ['Food', food])
            isoflavones = list_to_dict(isoflavones)
            dfisoflavones = pd.DataFrame([isoflavones])
            save_to_db(dfisoflavones, 'isoflavones', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        if oligosaccharides:
            oligosaccharides = convert_to_mg(oligosaccharides)
            oligosaccharides.insert(0, ['Food', food])
            oligosaccharides = list_to_dict(oligosaccharides)
            dfoligosaccharides = pd.DataFrame([oligosaccharides])
            save_to_db(dfoligosaccharides, 'oligosaccharides', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')

        df = pd.DataFrame(full_table_data, columns=header_list)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        df = pd.DataFrame()  # Return an empty DataFrame on error

    return df, 'foods/' + food + '.csv'

def read_file(file_path):
    with open(file_path, 'r') as file:
        variables = [line.strip() for line in file.readlines()]

    return variables    

def results_configurator():
    results_directory = './results/'
    current_datetime = datetime.now()
    formatted_datetime = current_datetime.strftime('%Y%m%d_%H%M%S')
    folder_name = results_directory + formatted_datetime
    os.makedirs(folder_name, exist_ok=True)

    return folder_name

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

def search_food(driver, food, folder_name):
    success = False
    while success == False:
        try:
            sleep(1)
            url = 'https://fdc.nal.usda.gov/fdc-app.html#/food-search?type=Foundation&query=' + food
            driver.get(url)

            sleep(1)
            # Locate the rows in the table body
            rows = driver.find_elements(By.XPATH, '//tbody[@_ngcontent-c3]/tr')

            descriptions = []

            if rows == []:
                with open(folder_name + '/missing_foods_' + folder_name.split('/')[-1] + '.txt', 'a') as file:
                    file.write(f'{food}\n')
            else:
                # Open the file in write mode
                with open(folder_name + '/corrected_foods_' + folder_name.split('/')[-1] + '.txt', 'a') as file:
                    # Iterate over each row, check if the description contains "almond", and write the first column (NDB Number) to the file
                    for row in rows:
                        description = row.find_element(By.XPATH, 'td[2]').text  # Locate the description column
                        # if food in description.lower():  # Check if 'almond' is in the description
                        #     ndb_number = row.find_element(By.XPATH, 'td[1]').text  # Locate the NDB Number column
                        file.write(f'{description}\n')
                        descriptions.append(description)
                # Find the element by class name and name attribute
                for description in descriptions:
                    try:
                        link_element = driver.find_element(By.LINK_TEXT, description)
                        
                        # Once the element is present, get the href attribute
                        href_value = link_element.get_attribute('href')
                        with open(folder_name + '/urls_' + folder_name.split('/')[-1] + '.txt', 'a') as file:
                            file.write(f'{href_value}\n')

                    except Exception as e:
                        logging.error(f"An error occurred: {e}")

            success = True
            sleep(1)
        except Exception as e:
            logging.error(f'error in the driver {e}')                   

def main():
    # Configure and initialize the logger file
    log_configurator()

    # Configure the  folder where to put the results
    folder_name = results_configurator()

    # Set up the Driver
    try:
        # Set up the Firefox WebDriver 
        driver_path = GeckoDriverManager().install()
        service = Service(executable_path=driver_path)
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')  # Run in headless mode (no GUI)
        driver = webdriver.Firefox(service=service, options=options)
    except Exception as e:
        logging.error(f'Firefox driver not found: {e}')
        try:
            # Set up the Chrome WebDriver 
            service = Service(ChromeDriverManager().install())
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')  # Run in headless mode (no GUI)

            driver = webdriver.Chrome(service=service, options=options)
        except Exception as e:
            logging.error(f'Chrome driver not found: {e}')
            try:
                # Set up the Chrome WebDriver 
                service = Service(EdgeChromiumDriverManager().install())
                options = webdriver.EdgeOptions()
                options.add_argument('--headless')  # Run in headless mode (no GUI)

                driver = webdriver.Edge(service=service, options=options)
            except Exception as e:
                logging.error(f'Edge driver not found: {e}')
    finally:
        driver.quit()

    # Read eated foods or the corrected Version
    if CORRECTED_FOODS == '' and URLS == '':
        foods = read_file('foods.txt')
    elif URLS != '':
        foods = []
    else:
        foods = read_file(CORRECTED_FOODS)

    #Obtaining URLS from file
    try:
        # Initialize the WebDriver
        try:
            driver = webdriver.Firefox(service=service, options=options)
        except:
            try:
                driver = webdriver.Chrome(service=service, options=options)
            except:
                driver = webdriver.Edge(service=service, options=options)

        for food in foods:
            search_food(driver, food, folder_name)

        driver.quit()
    except Exception as e:
        logging.error(f'error in the driver {e}')
    finally:
        # Ensure the WebDriver is properly closed
        driver.quit()


    # Read URLs created from eated foods
    if URLS == '':
        urls = read_file(folder_name + '/urls_' + folder_name.split('/')[-1] + '.txt')
    else:
        urls = read_file(URLS)

    # Search the informations in the URLs
    for url in urls:
        try:
            # Initialize the WebDriver
            try:
                driver = webdriver.Firefox(service=service, options=options)
            except:
                try:
                    driver = webdriver.Chrome(service=service, options=options)
                except:
                    driver = webdriver.Edge(service=service, options=options)
                    
            df, output_csv_path = extract_table_data(driver, url, folder_name)

            # Save the DataFrame to a CSV file
            df.to_csv(output_csv_path, index=False)
            logging.info(f"Data successfully saved to {output_csv_path}")

            driver.quit()
        except:
            logging.error('error in the driver')
        finally:
            # Ensure the WebDriver is properly closed
            driver.quit()

if __name__ == '__main__':
    main()