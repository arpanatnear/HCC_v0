# HCC_v0
HCC : high confidence cohort (Attribution  Panel) 

## To come up with a cohort with high ping density for a selected country and duration

# inputs:
Currently using (daily) staypoint output data 

# summary  
- Read the one day s.p. data for xmode,rtb and cubiq 
- Create time index for pings with midnight start as the refrence 
- ifa + index level aggregataion : pings_index, gh7s_index (unique) and count_index (present) in that day 
- ifa :count_index (present in that day ), pings_day 
- cnt_index vs cnt_ifa disn for that day 
- Write daily_counts to s3 : Master DB for HCC tunning
- loop for any given date and number of days
