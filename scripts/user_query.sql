-- User
-- Mobile user
select t1.year, t1.quarter, t1.brand_type, sum(t1.count), avg(t1.percentage) from phone_pe_pulse_data.tbl_aggregated_user t1 group by t1.year, t1.quarter, t1.brand_type;

-- ALL REGISTERED USER
select year, quarter, sum(registered_users) as registered_users, sum(app_opens) as app_opens from phone_pe_pulse_data.tbl_map_user 
group by year, quarter having year=2023 and quarter = 3;

-- STATE EACH REGISTERED USER
select  t1.year, t1.quarter, t2.state_name, sum(t1.registered_users) as registered_users, sum(t1.app_opens) as app_opens from phone_pe_pulse_data.tbl_map_user t1 
inner join phone_pe_pulse_data.tbl_states t2 on t1.state_id = t2.state_id
group by t1.year, t1.quarter, t1.state_id HAVING t1.year=2023 and t1.quarter = 3;


-- top state registered users

select t1.year, t1.quarter, t2.state_name, sum(t1.registered_users) as reg from phone_pe_pulse_data.tbl_top_user_district t1 
inner join phone_pe_pulse_data.tbl_states t2 on t1.state_id = t2.state_id group by t1.year, t1.quarter, t1.state_id 
HAVING t1.year=2023 and t1.quarter = 3 order by reg DESC limit 10;

-- top 10 districts registered users
select t2.district_name, t1.year, t1.quarter, registered_users, t1.district_id from phone_pe_pulse_data.tbl_top_user_district t1 
inner join phone_pe_pulse_data.tbl_districts t2 on t1.district_id = t2.district_id group by t1.year, t1.quarter, t1.district_id
HAVING t1.year=2023 and t1.quarter = 3 order by registered_users DESC limit 10;


-- top 10 pincode registered users

select year, quarter, pincode, sum(registered_users) as reg from phone_pe_pulse_data.tbl_top_user_pincode group by year, quarter, 
pincode HAVING year=2023 and quarter=3 order by reg desc limit 10;