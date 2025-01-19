------------------------------------------------
-----      LAB 2: Fact Data Modeling       -----
------------------------------------------------
/*
Blurry line between fact and dimension:
	- The log in event would be a fact that informs the "dim_is_active" dimension
	- VS the state "dim_is_activated" which is something that is state-driven, not activity driven
	- Example: Some users decide to deactivate their facebook profile ("dim_is_activated") but still preserve messenger and text ("dim_is_active")
Aggregate facts to turn them into dimensions: Bucketize aggregated facts can be useful to reduce cardinality

Properties of Facts vs Dimensions:
	- Dimensions:
		- Usually used for GROUP BY when doing analytics
		- Can be high or low cardinality
		- Generally come from a snapshot of state at date X
	- Facts:
		- Usually aggregated when doing analytics by SUM, AVG, COUNT, etc.
		- Typically higher volume than dimensions
		- Generally come from Logs: Event --> Log --> Fact

Datelist Ints:
	- https://www.linkedin.com/pulse/datelist-int-efficient-data-structure-user-growth-max-sung/
	- A Datelist Int is a data structure that encodes multiple days of user activity in a single integer value (usually a BIGINT). Each bit in the integer represents a calendar date for a rolling history, with the smallest bit representing the current date. If the user was active on a particular date, that bit is set to 1, otherwise it is set to 0. 
	- For example, in our sample active_user_datelist data above, user 123 was active on the current date (2022-02-13), 1 day ago (2022-02-12), and 3 days ago (2022-02-10). Each of these days is translated into a flipped bit, and the bit values are summed to generate the datelist int.

*/

-----------------------------------
----     Lab 2 - DateList      ----
-----------------------------------

-- Cumulative users table build from events
create table if not exists users_cumulated (
    user_id text,
    dates_active date[], -- The list of dates in the past where the user was active
    date date, -- Current date for the user
    primary key (user_id, date)
);

insert into users_cumulated
with yesterday as (
    select *
    from users_cumulated
    where date = date('2023-01-30') -- yesterday
), today as (
    select
        cast(user_id as text) as user_id,
        date(cast(event_time as timestamp)) as date_active
    from events
    where
        date(cast(event_time as timestamp)) = date('2023-01-31') -- today
        and user_id is not null -- deal with null user_ids in this data
    group by user_id, date(cast(event_time as timestamp))
)
select
    coalesce(t.user_id, y.user_id) as user_id,
    case
        when y.dates_active is null then array[t.date_active]
        when t.date_active is null then y.dates_active -- we don't want to keep adding a big array of nulls
        else array[t.date_active] || y.dates_active
    end as dates_active,
    -- today's date_active might not be date if the user doesn't exist yet
    -- so we add 1 to yesterday's
    coalesce(t.date_active, y.date + interval '1 day') as date
from today t full outer join yesterday y
on t.user_id = y.user_id;

-- Turn into a DateList: Bits where recent data is 1st or left and oldest data is the last bit or right one
-- generate a datelist for 30 days    
with users as (
    select * from users_cumulated
    where date = date('2023-01-31')
),
    series as (
        select * from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day') as series_date
    ),
    place_holder_ints as (
        select
            case
                when dates_active @> array[date(series_date)]
                    -- date - series_date is # of days b/e current date and series date
-- if we cast a power of 2 number as bits and turn it into binary
-- then we can get a history of 1s and 0s active/inactive
                    then cast(pow(2, 32 - (date - date(series_date))) as bigint)
                    else 0
                end as placeholder_int_value,
            *
        from users cross join series -- we got the 31 days for each user
    )
select
    user_id,
    -- these are extremely efficient operations
    -- bit_count() can give us how many times the user is active
    bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_is_monthly_active,
    -- let's check a user is active in the last 7 days
    bit_count(cast('11111110000000000000000000000000' as bit(32)) & --bit-wise AND for a week
        cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_is_weekly_active,
    -- daily is the same but with the 1st one only 1
    bit_count(cast('10000000000000000000000000000000' as bit(32)) & --bit-wise AND for a day
        cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_is_daily_active
from place_holder_ints
group by user_id;
