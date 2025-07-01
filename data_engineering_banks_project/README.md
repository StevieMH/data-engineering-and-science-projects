# Bank Data ETL Project

## Project Description

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline that scrapes data on the largest banks from a Wikipedia page, transforms the market capitalization data by converting USD values into GBP, EUR, and INR using up-to-date exchange rates, and loads the processed data into both a CSV file and a SQLite database.

The pipeline includes:
- Web scraping using Python's `requests` and `BeautifulSoup`
- Data transformation with `pandas` and `numpy`
- Logging of pipeline progress with timestamps
- Saving outputs as CSV and to an SQLite database
- Running basic SQL queries to validate and explore the data

This project demonstrates practical skills in web scraping, data cleaning, currency conversion, data storage, and basic SQL querying — key components in data engineering workflows.

---

## Technologies Used

- Python 3.x  
- pandas  
- numpy  
- requests  
- BeautifulSoup4  
- SQLite3  

---

