-- Transaction
use phone_pe_pulse_data;
select t1.year, t1.quarter, sum(t1.transaction_amount) as total_amount, avg(t1.transaction_amount) as avg_amount, 
sum(t1.transaction_count) as cnt from phone_pe_pulse_data.tbl_aggregated_transaction t1 group by t1.year, t1.quarter having t1.year=2024 and t1.quarter=1; 

select t1.year, t1.quarter, sum(t1.transaction_amount) as total_amount, avg(t1.transaction_amount) as avg_amount, 
sum(t1.transaction_count) as cnt from phone_pe_pulse_data.tbl_aggregated_transaction t1 where t1.year=2024 and t1.quarter=1; 


-- Categories

select t1.year, t1.quarter, t1.transaction_type, sum(transaction_amount) from phone_pe_pulse_data.tbl_aggregated_transaction t1 group by t1.year, t1.quarter, t1.transaction_type; 

-- map transaction for each states
select t2.state_name, t1.year, t1.quarter, sum(t1.transaction_count) as all_transaction, avg(t1.transaction_amount) as avg_amount 
from phone_pe_pulse_data.tbl_map_transaction t1 
inner join phone_pe_pulse_data.tbl_states t2 on t1.state_id = t2.state_id group by t1.year, t1.quarter, t2.state_id having t1.year=2023 and t1.quarter = 3;
	
-- select * from phone_pe_pulse_data.tbl_map_transaction where year=2024 and state_id=4;

-- Top 10 States
select t2.state_name, t1.quarter, t1.year, sum(t1.transaction_amount) as amount 
from phone_pe_pulse_data.tbl_top_transaction_district t1 inner join tbl_states t2 on t1.state_id = t2.state_id 
group by t1.year, t1.quarter, t1.state_id having t1.year=2023 and t1.quarter = 3 order by amount desc limit 10;

-- Top 10 Districts

select t1.state_id, t2.district_id, t2.district_name, t1.quarter, t1.year, sum(t1.transaction_amount) as amount 
from phone_pe_pulse_data.tbl_top_transaction_district t1 inner join phone_pe_pulse_data.tbl_districts t2 on t1.district_id = t2.district_id 
group by t1.year, t1.quarter, t1.district_id having t1.year=2023 and t1.quarter = 3 order by amount desc limit 10;


-- Top 10 pincodes

select state_id, year, quarter, pincode, sum(transaction_amount) as amount 
from phone_pe_pulse_data.tbl_top_transaction_pincode group by year, quarter, pincode having year=2023 and quarter = 3 order by amount desc limit 10;