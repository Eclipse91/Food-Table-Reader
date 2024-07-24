from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, exc, inspect, text
import pandas as pd
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

    # Create columns dynamically, all as String
    columns = [Column('Food', String, primary_key=True)]
    for col in df.columns:
        if col != 'Food':
            columns.append(Column(col, String))

    print("Columns for table:", columns)

    # Define the table schema
    table = Table(table_name, meta, *columns, extend_existing=True)

    # Create table in database
    try:
        meta.create_all(engine)
        print("Table created successfully")
    except exc.SQLAlchemyError as e:
        print(f"Error creating table: {e}")

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
            print(f"Error inserting data: {e}")


    print("Completed save_to_db function")

def extract_table_data(url):
    # Set up the Selenium WebDriver (Ensure the driver executable is in your PATH)
    driver = webdriver.Chrome()

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
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:']:
                            proximates.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Carbohydrates:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:']:
                            carbohydrates.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Minerals:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:']:
                            minerals.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Vitamins and Other Components:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:']:
                            vitamins.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Lipids:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:']:
                            lipids.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)
                    case 'Amino acids:':
                        if cell_data[0] not in ['Proximates:', 'Carbohydrates:', 'Minerals:', 'Vitamins and Other Components:', 'Lipids:', 'Amino acids:']:
                            amino_acids.append(cell_data)
                        else:
                            table_data = []
                            table_data.append(cell_data)

        # Create a DataFrame from the extracted data
        # if proximates:
        #     proximates = convert_to_mg(proximates)
        #     proximates.insert(0, ['Food', food])
        #     proximates = list_to_dict(proximates)
        #     dfproximates = pd.DataFrame(proximates)
        #     dfproximates.to_csv('components/proximates.csv', index=False)
        # if proximates:
        #     proximates = convert_to_mg(proximates)
        #     proximates.insert(0, ['Food', food])
        #     proximates_dict = list_to_dict(proximates)
        #     dfproximates = pd.DataFrame([proximates_dict])
        #     save_to_db(dfproximates)
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
            # dfcarbohydrates.to_csv('components/carbohydrates.csv', index=False)
        if minerals:
            minerals = convert_to_mg(minerals)
            minerals.insert(0, ['Food', food])
            minerals = list_to_dict(minerals)
            dfminerals = pd.DataFrame([minerals])
            save_to_db(dfminerals, 'minerals')
            # dfminerals.to_csv('components/minerals.csv', index=False)
        if vitamins:
            vitamins = convert_to_mg(vitamins)
            vitamins.insert(0, ['Food', food])
            vitamins = list_to_dict(vitamins)
            dfvitamins = pd.DataFrame([vitamins])
            save_to_db(dfvitamins, 'vitamins')
            # dfvitamins.to_csv('components/vitamins.csv', index=False)
        if lipids:
            lipids = convert_to_mg(lipids)
            lipids.insert(0, ['Food', food])
            lipids = list_to_dict(lipids)
            dflipids = pd.DataFrame([lipids])
            save_to_db(dflipids, 'lipids')
            # dflipids.to_csv('components/lipids.csv', index=False)
        if amino_acids:
            amino_acids = convert_to_mg(amino_acids)
            amino_acids.insert(0, ['Food', food])
            amino_acids = list_to_dict(amino_acids)
            dfamino_acids = pd.DataFrame([amino_acids])
            save_to_db(dfamino_acids, 'amino_acids')
            # dfamino_acids.to_csv('components/amino_acids.csv', index=False)

        df = pd.DataFrame(full_table_data, columns=header_list)

    except Exception as e:
        print(f"An error occurred: {e}")
        df = pd.DataFrame()  # Return an empty DataFrame on error

    finally:
        # Ensure the WebDriver is properly closed
        driver.quit()

    return df, 'foods/' + food + '.csv'

def read_urls_file(file_path):
    with open(file_path, 'r') as file:
        urls = [line.strip() for line in file.readlines()]
    return urls    


if __name__ == '__main__':
    urls = read_urls_file('urls.txt')
    for url in urls:
        df, output_csv_path = extract_table_data(url)

        # Save the DataFrame to a CSV file
        df.to_csv(output_csv_path, index=False)
        print(f"Data successfully saved to {output_csv_path}")
