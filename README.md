# Phone Pe Pulse Data Visualization using SQL, Streamlit, Pandas, Plotly

## Tech Stack: Github Cloning, Python scripting, Phonepe pulse Data Collection, Streamlit, Pymysql, Pandas, Sqlalchemy, Plotly

## FinTech

In The Project we have Streamlit used to build the Geo India Map Location application responsively


## Project Structure
- Phone Pe Pulse Data Folder
  - pluse folder
      - data
        - aggregated
            - Related state/state_name/.json
        - Map
            - Related state/state_name/.json
        - top
            - Related state/state_name/.json
  - Scripts
      - transaction_query.sql
      - user_query.sql
      - insurance_quer.sql
  - main.py
  - database_connection.py
  - extraction_data.py
  - pulse_data_queries.py
  - requirements.txt
  - schemas.py
            
  Extarct Data from given pulse data fodler. then hit database and get records, then i will show records in form of Geo india Map and UI


  - UI Parts file
      - main.py
          - To choose the pulse action, year and quarter to get records from the database and show the aggregated data in form of Map and table format.
  - scripts/.sql
      - To create the raw aggregate the queries for the reference to build the sqlalchemy orm
  - database_connection.py
    - It is used connect the database in given sqlalchemy server, then insert and get the record from database
  - extraction_data.py
    - It is used to extract the data from pluse data folder and to form of dataframe using pandas.
  - pulse_data_queries.py
    - It is used to build sqlalchemy orm queries for all actions.
  - requirements.txt
    - It is used to install package for this project needs.
  - schemas.py
    - It is used to frame the sqlalchemy orm class to build orm queries using sqlalchemy.
  
