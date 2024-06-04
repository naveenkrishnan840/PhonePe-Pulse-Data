use phone_pe_pulse_data;

-- All Insurance
select year, quarter, sum(insurance_count) as no_of_policies, sum(insurance_amount) as total_premium_value, avg(insurance_amount) as avg_premium_value 
from phone_pe_pulse_data.tbl_aggregated_insurance group by year, quarter HAVING year=2024 and quarter=1;



-- Map Each sate insurance
select t2.state_name, t1.year, t1.quarter, sum(t1.insurance_count), sum(t1.insurance_amount), avg(t1.insurance_amount) 
from phone_pe_pulse_data.tbl_map_insurance t1 inner join phone_pe_pulse_data.tbl_states t2 on t1.state_id = t2.state_id
 group by year, quarter, t1.state_id HAVING t1.year=2024 and t1.quarter=1;
 
 -- top 10 state
 select t2.state_name, sum(t1.insurance_amount) as avg_amount from phone_pe_pulse_data.tbl_top_insurance_district t1 
 inner join phone_pe_pulse_data.tbl_states t2 on t1.state_id = t2.state_id group by t1.year, t1.quarter, t1.state_id 
 HAVING t1.year=2024 and t1.quarter=1 order by avg_amount DESC limit 10;
 
 
 -- top 10 districts
  select t2.district_name, sum(t1.insurance_amount) as avg_amount from phone_pe_pulse_data.tbl_top_insurance_district t1 
 inner join phone_pe_pulse_data.tbl_districts t2 on t1.district_id = t2.district_id group by t1.year, t1.quarter, t1.district_id
 HAVING t1.year=2024 and t1.quarter=1 order by avg_amount DESC limit 10;
 
 
 -- top 10 pincodes
  select t1.pincode, sum(t1.insurance_amount) as avg_amount from phone_pe_pulse_data.tbl_top_insurance_pincode t1 group by t1.year, t1.quarter, t1.pincode
 HAVING t1.year=2024 and t1.quarter=1 order by avg_amount DESC limit 10;
 
	