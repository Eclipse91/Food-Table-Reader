# Food Table Reader Project

## Overview
This script is designed to extract nutritional data from a given `foods.txt` file containing various foods, process the data to convert measurements into milligrams (mg), and save the data to a SQLite database. Any value is based on 100 grams of the product.

## Why didn't I use the API?
At the time, I wanted to create a project using Selenium and also analyze my diet deeply to look for deficiencies. That's why I created this slow program that still works effectively.

## Features
1. **Web Scraping**: Utilizes Selenium WebDriver to scrape nutritional data from web pages.
2. **Data Processing**: Converts nutritional values to a consistent unit (mg) for uniformity.
3. **Database Storage**: Stores the processed data in a SQLite database for easy querying and analysis.

## Requirements

- Python 3.x
- Required Python packages are listed in the requirements.txt file.
- Google Chrome, Firefox, or Edge should be installed, or you should have the respective driver. If the driver is used, you'll need to update the code with the path to it.

## Drivers Download
- Firefox: https://github.com/mozilla/geckodriver/releases
   Ensure that the GeckoDriver version is compatible with your Firefox browser version.
- Chrome: https://googlechromelabs.github.io/chrome-for-testing/
   Make sure the ChromeDriver version aligns with your Chrome browser version. 
- Microsoft Edge: https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/?form=MA13LH
   Confirm that the EdgeDriver version matches your Microsoft Edge browser version.
   
## Getting Started

1. **Clone the repository**:
   ```bash
   git clone https://github.com/Eclipse91/Food-Table-Reader.git
   ```

2. **Navigate to the project directory**:
   ```bash
   cd Food-Table-Reader
   ```   

3. **Install the required dependencies** (creating a virtual environment is strongly recommended):
   ```bash
   pip install -r requirements.txt
   ```

4. **Create a `foods.txt` file**: Add the foods you usually eat, one per line.

5. **Run the application**:
   ```bash
   python3 main.py
   ```

## Required Files
- `foods.txt`: A text file containing the list of foods you usually eat, one per line.

## Optional Files to Speed Up
- `corrected_foods.txt`: A text file containing the list of foods which you are sure are present in the U.S. Department of Agriculture database with that name. Add its path to the CORRECTED_FOODS constant in the main.py file.
- `urls.txt`: A text file containing the list of URLs of the foods you want to analyze in the U.S. Department of Agriculture database. Add its path to the URLS constant in the main.py file.

## Usage
1. **Prepare the URL List**: Ensure `foods.txt` is filled with the foods you eat regularly.
2. **Run the Script**:
   ```bash
   python3 main.py
   ```
3. **Compare the values in the db**: Check if some value is less than expected compared with your RDA.

## License
This project is licensed under the GNU General Public License - see the [LICENSE](LICENSE) file for details.

## Notes
Feel free to contribute or report issues! This README provides a clear structure, concise information, and instructions for setting up and running the Food Table Reader. Adjust the content as needed for your project.

## Acknowledgements
- [U.S. Department of Agriculture](https://fdc.nal.usda.gov/fdc-app.html#/food-search?query=&type=Foundation)

