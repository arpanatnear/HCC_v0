# HCC_v0
HCC : high confidence cohort (Attribution  Panel) 

## To come up with a cohort with high ping density for a selected country and duration

# input data:
Currently using (daily) staypoint output data 

HCC_vo_build_master_db.py : Based on the given inputs, this will fetch the staypoints data and write the summarised data.

# steps 
- Read the one day s.p. data for xmode,rtb and cubiq 
- Create time index for pings with midnight start as the refrence 
- ifa + index level aggregataion : pings_index, gh7s_index (unique)
- ifa : count_index (present) in that day , pings_day 
- cnt_index vs cnt_ifa disn for that day 
- Write daily_counts to s3 : Master DB for HCC tunning
- loop for any given date and number of days

HCC_vo_aggn_from_master_db.py : this will fetch the above master data, apply the condition inputs and write the resulatant data to s3.
