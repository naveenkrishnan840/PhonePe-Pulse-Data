create database phone_pe_pulse_data;

drop database phone_pe_pulse_data;
use phone_pe_pulse_data;

drop table tbl_states;
drop table tbl_districts;
-- CREATE TABLE IF NOT EXISTS tbl_states (state_id INT PRIMARY KEY UNIQUE, state_name VARCHAR(100) NOT NULL);
-- CREATE TABLE IF NOT EXISTS tbl_districts (district_id INT PRIMARY KEY UNIQUE, district_name VARCHAR(100) NOT NULL);

-- tbl_aggregated_transaction
CREATE TABLE IF NOT EXISTS tbl_aggregated_transaction (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, transaction_type VARCHAR(50) NOT NULL
, transaction_count INT NOT NULL, transaction_amount INT NOT NULL, FOREIGN KEY (state_id) REFERENCES tbl_states(state_id));


-- tbl_aggregated_insurance
-- drop table tbl_aggregated_insurance;
CREATE TABLE IF NOT EXISTS tbl_aggregated_insurance (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, 
insurance_count INT NOT NULL, insurance_amount INT NOT NULL, FOREIGN KEY (state_id) REFERENCES tbl_states(state_id));



-- tbl_aggregated_user
-- drop table tbl_aggregated_user;
CREATE TABLE IF NOT EXISTS tbl_aggregated_user (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, brand_type VARCHAR(50) NOT NULL,
count INT NOT NULL, percentage INT NOT NULL, FOREIGN KEY (state_id) REFERENCES tbl_states(state_id));



-- tbl_map_transaction 
-- drop table tbl_map_transaction;
CREATE TABLE IF NOT EXISTS tbl_map_transaction (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, district_id INT NOT NULL
, transaction_count INT NOT NULL, transaction_amount INT NOT NULL, FOREIGN KEY (district_id) REFERENCES tbl_districts(district_id), FOREIGN KEY (state_id) REFERENCES tbl_states(state_id));

-- tbl_map_insurance
-- drop table tbl_map_insurance;

CREATE TABLE IF NOT EXISTS tbl_map_insurance (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, district_id INT NOT NULL,
insurance_count INT NOT NULL, insurance_amount INT NOT NULL, FOREIGN KEY (district_id) REFERENCES tbl_districts(district_id), FOREIGN KEY (state_id) REFERENCES tbl_states(state_id));

-- tbl_map_user
-- drop table tbl_map_user;

CREATE TABLE IF NOT EXISTS tbl_map_user (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, district_id INT NOT NULL,
registered_users INT NOT NULL, app_opens INT NOT NULL, FOREIGN KEY (district_id) REFERENCES tbl_districts(district_id), FOREIGN KEY (state_id) REFERENCES tbl_states(state_id));

-- tbl_top_transaction_district
-- drop table tbl_top_transaction_district;

CREATE TABLE IF NOT EXISTS tbl_top_transaction_district (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, district_id INT NOT NULL,
transaction_count INT NOT NULL, transaction_amount INT NOT NULL, FOREIGN KEY (state_id) REFERENCES tbl_states(state_id), FOREIGN KEY (district_id) REFERENCES tbl_districts(district_id));

-- tbl_top_transaction_pincode
-- drop table tbl_top_transaction_pincode;

CREATE TABLE IF NOT EXISTS tbl_top_transaction_pincode (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, pincode VARCHAR(100) NOT NULL,
transaction_count INT NOT NULL, transaction_amount INT NOT NULL, FOREIGN KEY (state_id) REFERENCES tbl_states(state_id));


-- tbl_top_user_district
-- drop table tbl_top_user_district;

CREATE TABLE IF NOT EXISTS tbl_top_user_district (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, district_id INT NOT NULL,
registered_users INT NOT NULL, FOREIGN KEY (state_id) REFERENCES tbl_states(state_id), FOREIGN KEY (district_id) REFERENCES tbl_districts(district_id));

-- tbl_top_user_pincodes
-- drop table tbl_top_user_pincode;

CREATE TABLE IF NOT EXISTS tbl_top_user_pincode (id INT AUTO_INCREMENT PRIMARY KEY, state_id INT NOT NULL, year INT NOT NULL, quarter INT NOT NULL, pincode VARCHAR(100) NOT NULL,
registered_users INT NOT NULL, FOREIGN KEY (state_id) REFERENCES tbl_states(state_id));

show tables;

select * from tbl_states;
select * from tbl_districts;
select * from tbl_aggregated_transaction where year=2024 and quarter=1 and state_id=4;
select * from tbl_top_user_pincode;
select * from tbl_top_user_district;
select * from tbl_top_transaction_pincode;
select * from tbl_top_transaction_district;
select * from tbl_top_insurance_district;
select * from tbl_top_insurance_pincode;
select * from tbl_map_user;
select * from tbl_aggregated_insurance;
select * from tbl_aggregated_user;
select * from tbl_map_transaction;
select * from tbl_map_insurance;
select * from tbl_aggregated_insurance where state_id=4 and year=2024 and quarter=4;
describe tbl_aggregated_insurance;
describe tbl_aggregated_transaction;


drop tables tbl_aggregated_transaction;
drop tables tbl_top_user_pincode;
drop tables tbl_top_user_district;
drop tables tbl_top_insurance_district;
drop tables tbl_top_insurance_pincode;
drop tables tbl_top_transaction_pincode;
drop tables tbl_top_transaction_district;
drop tables tbl_map_user;
drop tables tbl_aggregated_insurance;
drop tables tbl_aggregated_user;
drop tables tbl_map_transaction;
drop tables tbl_map_insurance;
drop tables tbl_states;
drop tables tbl_districts;
show tables;