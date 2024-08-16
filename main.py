import os
import logging
from datetime import datetime
import tables_reader

# Adda list of valid urls. Check example_urls.txt or directly the USDA Site
URLS = 'example_urls.txt' # 'example_urls.txt'
# Adda list of valid foods. Check example_corrected_foods.txt or directly the USDA Site
CORRECTED_FOODS = '' # 'example_corrected_foods.txt'

def read_file(file_path):
    '''
    Read the files with the foods or URLs and return their content as a list.
    '''
    with open(file_path, 'r') as file:
        variables = [line.strip() for line in file.readlines()]

    return variables    

def results_configurator():
    '''
    Configure the folder where to put all the resulting files.
    '''
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
        filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s'
        )
    logging.info('Program started')

def execution_time(func):
    '''
    Decorator that prints the current date and time before and after
    executing the given function, and measures the time taken for execution.
    The datetime format is 'YYYYMMDD_HHMMSS'.
    '''
    def wrapper():
        starting_datetime = datetime.now()
        formatted_datetime = starting_datetime.strftime('%Y%m%d_%H%M%S')
        print(f'Program started at {formatted_datetime}\n')
        func()
        current_datetime = datetime.now()
        formatted_datetime = current_datetime.strftime('%Y%m%d_%H%M%S')
        print(f'Program ended at {formatted_datetime}\n')
        print(f'Total time elapsed: {current_datetime - starting_datetime}')

    return wrapper

@execution_time
def main():
    # Configure and initialize the logger file
    log_configurator()

    # Configure the folder where to put the results
    folder_name = results_configurator()

    # Set up the Driver
    service, options = tables_reader.set_up_driver()

    if service != '' and options != '':
        # Read eated foods or the corrected Version or None of them
        if CORRECTED_FOODS == '' and URLS == '':
            foods = read_file('foods.txt')
        elif URLS != '':
            foods = []
        else:
            foods = read_file(CORRECTED_FOODS)

        #Obtaining URLS from file
        try:
            # Initialize the WebDriver
            driver = tables_reader.initialize_driver(service, options)

            counter = 0
            for food in foods:
                counter += 1
                if counter == 50:
                    counter = 0
                    driver = tables_reader.initialize_driver(service, options)

                tables_reader.search_food(driver, food, folder_name)

            driver.quit()
        except Exception as e:
            logging.error(f'error in the driver while searching "{food}": {e}')
        finally:
            driver.quit()

        # Read URLs created from eated foods or URLS file
        if URLS == '':
            urls = read_file(folder_name + '/urls_' + folder_name.split('/')[-1] + '.txt')
        else:
            urls = read_file(URLS)

        # Search the informations in the URLs
        for url in urls:
            try:
                # Initialize the WebDriver
                driver = tables_reader.initialize_driver(service, options)
                        
                df, csv_name = tables_reader.extract_table_data(driver, url, folder_name)
                
                os.makedirs('foods', exist_ok=True)
                csv_name = 'foods/' + csv_name.replace('/','')

                # Save the DataFrame to a CSV file
                df.to_csv(csv_name, index=False)
                logging.info(f'Data successfully saved to "{csv_name}"\n')
            except:
                logging.error(f'error in the driver while using {url}\n')
            finally:
                # Ensure the WebDriver is properly closed
                driver.quit()
        
        logging.info('Program ended successfully')

def unique_foods_creator():
    file_1 = read_file('example_urls.txt')
    file_2 = read_file('example_urls.txt')
    foods = file_1 + file_2

    unique_foods = {}
    for food in foods:
        unique_foods[food] = 0

    for food in unique_foods:
        with open('unique_foods.txt', 'a') as file:
            file.write(f'{food}\n')

if __name__ == '__main__':
    main()