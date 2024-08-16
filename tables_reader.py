import os
import logging
from time import sleep
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
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, exc, inspect, text, select, func
from sqlalchemy.exc import SQLAlchemyError

def convert_to_mg(data):
    '''
    Convert data to mg if necessary.
    '''
    processed_data = []
    for mineral, value, unit in data:
        if '<' in value:
            numeric_value = float(value.replace('<', ''))
            if unit == 'µg':
                value_mg = numeric_value / 1000  # Convert µg to mg
            elif unit == 'g':
                value_mg = numeric_value * 1000  # Convert g to mg
            else:
                value_mg = numeric_value
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
    '''
    Transform a list into a dictionary in order to convert it into a pandas DataFrame.
    '''
    food_dict = {}
    for item in data:
        key = item[0]
        value = item[1]
        food_dict[key] = value
    return food_dict

def get_record_count(connection, table):
    '''
    Get the count of records in a specified database table.
    '''
    count_stmt = select((func.count())).select_from(table)
    result = connection.execute(count_stmt)
    count = result.scalar()
    return count

def save_to_db(df, table_name, db_path='sqlite:///food_components.db'):
    '''
    Save a DataFrame into its specific table in food_components.db.
    '''
    logging.info('Starting save_to_db function')
    engine = create_engine(db_path)
    meta = MetaData()
    inspector = inspect(engine)

    # Define initial columns (assuming 'Food' as the primary key)
    initial_columns = [Column('Food', String, primary_key=True)]
    for col in df.columns:
        if col != 'Food':
            initial_columns.append(Column(col, Float))  # Use Float instead of String

    logging.info('Initial columns for table')

    with engine.connect() as connection:
        # Check if the table exists
        if not connection.dialect.has_table(connection, table_name):
            # Define the table schema
            table = Table(table_name, meta, *initial_columns, extend_existing=True)
            # Create table in database if it doesn't exist
            try:
                meta.create_all(engine)
                logging.info('Table created successfully')
            except exc.SQLAlchemyError as e:
                logging.error(f'Error creating table {table_name}: {e}')
        else:
            logging.info('Table already exists')
            # Get existing columns
            existing_columns = inspector.get_columns(table_name)
            existing_column_names = [col['name'] for col in existing_columns]
            logging.info('Existing columns')

            # Find new columns to add
            new_columns = []
            for col in df.columns:
                if col not in existing_column_names:
                    new_columns.append(Column(col, Float))  # Ensure new columns are added as Float
            
            # Add new columns if any
            if new_columns:
                logging.info(f'New columns to add: {new_columns}')
                for column in new_columns:
                    alter_stmt = text(f'ALTER TABLE {table_name} ADD COLUMN "{column.name}" FLOAT')  # Use FLOAT for new columns
                    try:
                        connection.execute(alter_stmt)
                        logging.info(f'Added column {column.name} of type {column.type} to table {table_name}')
                    except exc.SQLAlchemyError as e:
                        logging.error(f'Error adding column {column.name}: {e}')

                # Update existing rows with default values for new columns
                for column in new_columns:
                    update_stmt = text(f'UPDATE {table_name} SET "{column.name}" = 0')  # Set default value to 0
                    try:
                        connection.execute(update_stmt)
                        logging.info(f'Updated existing rows with default value for column {column.name}')
                    except exc.SQLAlchemyError as e:
                        logging.error(f'Error updating existing rows for column {column.name}: {e}')

        # Insert or update the records
        table = Table(table_name, meta, autoload_with=engine)

    # Insert or update the record
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            # Count records before insertion
            count_before = get_record_count(connection, table)
            logging.info(f'Record count before insertion: \t{count_before}')

            for _, row in df.iterrows():
                data = row.to_dict()
                # Convert all data to float (assuming all columns except 'Food' should be float)
                data = {key: float(value) if key != 'Food' else value for key, value in data.items()}
                logging.info(f'Data to insert: {data}')
                stmt = table.insert().values(data).prefix_with('OR REPLACE')
                logging.info('SQL Statement')
                connection.execute(stmt)
            transaction.commit()
            logging.info(f'Data {data["Food"]} inserted successfully to "{table_name}"')

            # Count records after insertion
            count_after = get_record_count(connection, table)
            logging.info(f'Record count after insertion: \t\t{count_after}')

            if count_after == count_before:
                raise SQLAlchemyError('Record count did not change after insertion')

        except SQLAlchemyError as e:
            transaction.rollback()
            logging.error(f'Error inserting data "{table_name}": {e}')

    logging.info(f'Completed save_to_db function of "{table_name}"')

def save_to_csv(data, file_path):
    '''
    Save a DataFrame to food_components.csv.
    '''
    if os.path.exists(file_path):
        # If the file exists, read it into a DataFrame
        existing_data = pd.read_csv(file_path)
        # Append the new data, aligning columns and filling missing values with NaN
        combined_data = pd.concat([existing_data, data], ignore_index=True)
        # Save the combined DataFrame back to the CSV file
        combined_data.to_csv(file_path, mode='w', header=True, index=False)
    else:
        # If the file does not exist, create it and write the header
        data.to_csv(file_path, mode='w', header=True, index=False)

def extract_table_data(driver, url, folder_name):
    '''
    Extract the data from the tables using Selenium and organize them into 
    specific DataFrames in order to create .csv and .db files.
    '''
    try:
        # Navigate to the URL
        driver.get(url)

        # Wait for the table header to be present
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.XPATH, '//thead//th')))

        # Path of the file
        food = driver.find_element(By.ID, 'foodDetailsDescription').text

        # Locate the table header
        headers = driver.find_elements(By.XPATH, '//thead//th')

        # Extract the headers (only first 3 headers)
        header_list = [header.text.strip() for header in headers[:3]]

        # Locate the table rows
        rows = driver.find_elements(By.XPATH, '//tbody//tr')

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
            cells = row.find_elements(By.XPATH, './/td')
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
            save_to_csv(dfproximates, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        if carbohydrates:
            carbohydrates = convert_to_mg(carbohydrates)
            carbohydrates.insert(0, ['Food', food])
            carbohydrates = list_to_dict(carbohydrates)
            dfcarbohydrates = pd.DataFrame([carbohydrates])
            save_to_db(dfcarbohydrates, 'carbohydrates', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')
            save_to_csv(dfcarbohydrates, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        if minerals:
            minerals = convert_to_mg(minerals)
            minerals.insert(0, ['Food', food])
            minerals = list_to_dict(minerals)
            dfminerals = pd.DataFrame([minerals])
            save_to_db(dfminerals, 'minerals', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')
            save_to_csv(dfminerals, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        if vitamins:
            vitamins = convert_to_mg(vitamins)
            vitamins.insert(0, ['Food', food])
            vitamins = list_to_dict(vitamins)
            dfvitamins = pd.DataFrame([vitamins])
            save_to_db(dfvitamins, 'vitamins', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')
            save_to_csv(dfvitamins, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        if lipids:
            lipids = convert_to_mg(lipids)
            lipids.insert(0, ['Food', food])
            lipids = list_to_dict(lipids)
            dflipids = pd.DataFrame([lipids])
            save_to_db(dflipids, 'lipids', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')
            save_to_csv(dflipids, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        if amino_acids:
            amino_acids = convert_to_mg(amino_acids)
            amino_acids.insert(0, ['Food', food])
            amino_acids = list_to_dict(amino_acids)
            dfamino_acids = pd.DataFrame([amino_acids])
            save_to_db(dfamino_acids, 'amino_acids', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')
            save_to_csv(dfamino_acids, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        if phytosterols:
            phytosterols = convert_to_mg(phytosterols)
            phytosterols.insert(0, ['Food', food])
            phytosterols = list_to_dict(phytosterols)
            dfphytosterols = pd.DataFrame([phytosterols])
            save_to_db(dfphytosterols, 'phytosterols', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')
            save_to_csv(dfphytosterols, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        if organic_acids:
            organic_acids = convert_to_mg(organic_acids)
            organic_acids.insert(0, ['Food', food])
            organic_acids = list_to_dict(organic_acids)
            dforganic_acids = pd.DataFrame([organic_acids])
            save_to_db(dforganic_acids, 'organic_acids', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')
            save_to_csv(dforganic_acids, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        if isoflavones:
            isoflavones = convert_to_mg(isoflavones)
            isoflavones.insert(0, ['Food', food])
            isoflavones = list_to_dict(isoflavones)
            dfisoflavones = pd.DataFrame([isoflavones])
            save_to_db(dfisoflavones, 'isoflavones', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')
            save_to_csv(dfisoflavones, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        if oligosaccharides:
            oligosaccharides = convert_to_mg(oligosaccharides)
            oligosaccharides.insert(0, ['Food', food])
            oligosaccharides = list_to_dict(oligosaccharides)
            dfoligosaccharides = pd.DataFrame([oligosaccharides])
            save_to_db(dfoligosaccharides, 'oligosaccharides', 'sqlite:///' + folder_name + '/food_components_' + folder_name.split('/')[-1] + '.db')
            save_to_csv(dfoligosaccharides, folder_name + '/food_components_' + folder_name.split('/')[-1] + '.csv')

        df = pd.DataFrame(full_table_data, columns=header_list)
        logging.info('Completed Extraction')

    except Exception as e:
        logging.error(f'An error occurred with "{food}": {e}')
        df = pd.DataFrame()  # Return an empty DataFrame on error

    return df, food + '.csv'

def search_food(driver, food, folder_name):
    '''
    Search for foods using Selenium and output 3 files:
    - missing_foods: for foods that couldn't be found
    - corrected_foods: using the names found on the site
    - urls: containing the links of all the foods found
    '''
    success = False
    while success == False:
        try:
            sleep(1)
            logging.info(food)
            url = 'https://fdc.nal.usda.gov/fdc-app.html#/food-search?type=Foundation&query=' + food
            driver.get(url)
            driver.refresh()

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
                    # Iterate over each row, check if the description contains 'almond', and write the first column (NDB Number) to the file
                    for row in rows:
                        description = row.find_element(By.XPATH, 'td[2]').text  # Locate the description column
                        # if food in description.lower():  # Check if 'almond' is in the description
                        #     ndb_number = row.find_element(By.XPATH, 'td[1]').text  # Locate the NDB Number column
                        file.write(f'{description}\n')
                        descriptions.append(description)
                logging.info(description)
                # Find the element by class name and name attribute
                for description in descriptions:
                    try:
                        link_element = driver.find_element(By.LINK_TEXT, description)
                        
                        # Once the element is present, get the href attribute
                        href_value = link_element.get_attribute('href')
                        with open(folder_name + '/urls_' + folder_name.split('/')[-1] + '.txt', 'a') as file:
                            file.write(f'{href_value}\n')

                        logging.info(href_value)
                    except Exception as e:
                        logging.error(f'An error occurred with "{food}": {e}')

            success = True
            sleep(1)
        except Exception as e:
            logging.error(f'error in the driver while connecting to the URL of "{food}": {e}')

def set_up_driver():
    '''
    Set up the Driver.
    '''
    try:
        # Set up the Firefox WebDriver 
        driver_path = GeckoDriverManager().install()
        service = Service(executable_path=driver_path)
        # service = Service(executable_path='geckodriver') # for me has to be in the root
        options = webdriver.FirefoxOptions()
        options.add_argument('--headless')  # Run in headless mode (no GUI)
        driver = webdriver.Firefox(service=service, options=options)

    except Exception as e:
        logging.error(f'Firefox driver not found: {e}')
        try:
            # Set up the Chrome WebDriver 
            service = Service(ChromeDriverManager().install())
            # driver_path = os.path.join(os.getcwd(), 'drivers', 'chromedriver')
            # service = Service(executable_path=driver_path)
            options = webdriver.ChromeOptions()
            options.add_argument('--headless')  # Run in headless mode (no GUI)
            driver = webdriver.Chrome(service=service, options=options)

        except Exception as e:
            logging.error(f'Chrome driver not found: {e}')
            try:
                # Set up the Edge WebDriver 
                service = Service(EdgeChromiumDriverManager().install())
               # driver_path = os.path.join(os.getcwd(), 'drivers', 'msedgedriver')
                options = webdriver.EdgeOptions()
                options.add_argument('--headless')  # Run in headless mode (no GUI)
                driver = webdriver.Edge(service=service, options=options)

            except Exception as e:
                logging.error(f'Edge driver not found: {e}')
                logging.error(f'Program ended because the WebDriver could not be found.')
                return '',''
            
    finally:
        driver.quit()

    return service, options

def initialize_driver(service, options):
    '''
    Initialize the WebDriver.
    '''
    try:
        driver = webdriver.Firefox(service=service, options=options)
    except:
        try:
            driver = webdriver.Chrome(service=service, options=options)
        except:
            driver = webdriver.Edge(service=service, options=options)

    return driver
