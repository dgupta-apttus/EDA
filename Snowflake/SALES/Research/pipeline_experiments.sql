Select "Opportunity ID", "Snapshot Date", "Stage", "Forecast Category", "Calc.ACV", CLOSEDATE_ADJ, "Close Date"
from APTTUS_DW.SF_PRODUCTION.SALES_PIPELINESNAPSHOT
where "Opportunity ID" IN ('0061U000002BSOUQA4')
  and "Snapshot Date" > '2020-09-20'
order by "Snapshot Date" desc  
;

SELECT 
"ACV (USD)", "First Year ACV (USD)", "Ramped ACV (USD)", "TCV (USD)", "Weighted ACV (USD)"
, "Snapshot Date", "Stage", "Forecast Category", "Close Date"
FROM APTTUS_DW.SF_PRODUCTION."SFDC_Lightning_Snapshot_Data_FY21"
where "OpportunityID" IN ('0061U000002BSOUQA4')
  and "Snapshot Date" > '2020-09-20'
order by "Snapshot Date" desc
;

--with get_weeks as (
        select MIN("Date") AS PeriodStartDate
             , MAX("Date") AS PeriodEndDate
             , "Fiscal Period"
             , "Week (Quarter)"  -- this is the old TB week
             --, soon there will be a new TB week   
        from "APTTUS_DW"."SF_PRODUCTION"."Dates" 
        where "Date" >='2020-02-01'
          and "Date" < current_date() 
        group by "Fiscal Period", "Week (Quarter)" -- change to new TB week
order by PeriodStartDate
--)
;


-- this could be a view definition that would align on the end of a weekley period, and report on open opps during the current Fiscal quarter
with current_period as (
        select "Fiscal Period"
             , "Week (Quarter)" -- this is the old TB week
             --, soon there will be a new TB week  
        from "APTTUS_DW"."SF_PRODUCTION"."Dates" 
        where "Date" = current_date() --+7  -- +7 is only here for testing, remove it later   
)
, current_week as (
        select MAX(D."Date") as "Current_WeekEndDate"
             , MIN(D."Date") as "Current_WeekStartDate"       
        from         "APTTUS_DW"."SF_PRODUCTION"."Dates" D
        inner join   current_period C
                   ON  D."Fiscal Period" = C."Fiscal Period"
                   AND D."Week (Quarter)" = C."Week (Quarter)"  -- switch to new TB week            
)
, current_quarter as (
        select MIN(D."Date") as "Current_FPStart"      
             , MAX(D."Date") as "Current_FPEnd"  
        from         "APTTUS_DW"."SF_PRODUCTION"."Dates" D
        inner join   current_period C
                   ON  D."Fiscal Period" = C."Fiscal Period"       
)
, relevant_periods as (
        select MIN("Date") AS PeriodStartDate
             , MAX("Date") AS PeriodEndDate
             , "Fiscal Period"
             , "Week (Quarter)"  -- this is the old TB week
             --, soon there will be a new TB week   
        from "APTTUS_DW"."SF_PRODUCTION"."Dates" 
        where "Date" >= (Select "Current_FPStart" from current_quarter)
          and "Date" <= (Select "Current_WeekEndDate" from current_week)  
        group by "Fiscal Period", "Week (Quarter)" -- change to new TB week
----order by PeriodStartDate -- don't sort in an interim step
)
, max_per_period as (
select MAX(A.SNAPSHOT_DATE) as PERIOD_MAX_SNAP
     , A.CRM_SOURCE
     , A.OPPORTUNITY_ID
     , B."Fiscal Period" 
     , B."Week (Quarter)"
from                        APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY A
inner join                  relevant_periods B
               on 1=1 -- yes the cartesian product is "on purpose"
where A.SNAPSHOT_DATE <= B.PeriodEndDate
  and A.CREATED_DATE <= B.PeriodStartDate
  and (A.CLOSE_BOOKINGS_DATE is null 
        OR A.CLOSE_BOOKINGS_DATE between (Select "Current_FPStart" from current_quarter) AND (Select "Current_FPEnd" from current_quarter)
      )     
group by A.CRM_SOURCE
     , A.OPPORTUNITY_ID
     , B."Fiscal Period" 
     , B."Week (Quarter)"               
)
-- test counts
--select count(*), "Week (Quarter)"
--from max_per_period
--group by "Week (Quarter)"                       

select B.ACCOUNT_NAME
     , B.OPPORTUNITY_NAME 
     , A.CRM_SOURCE
     , A.OPPORTUNITY_ID
     , A."Fiscal Period" 
     , A."Week (Quarter)"
     , B.C2_STAGE
     , B.FORECAST_CATEGORY
     , B.AVERAGE_ACV
     , B.FIRST_YEARS_BILLINGS
     , B.CREATED_DATE
     , B.CLOSE_BOOKINGS_DATE
     , B.ESTIMATED_CLOSE_DATE
     , B.SNAPSHOT_DATE  
from                        max_per_period A
inner join                  APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY B
             ON  A.CRM_SOURCE = B.CRM_SOURCE
             AND A.OPPORTUNITY_ID = B.OPPORTUNITY_ID
             AND A.PERIOD_MAX_SNAP = B.SNAPSHOT_DATE 
order by B.ACCOUNT_NAME, A.OPPORTUNITY_ID, A."Week (Quarter)"
;

with max_last_week as (
        select MAX(SNAPSHOT_DATE) as MAX_SNAP
             , CRM_SOURCE
             , OPPORTUNITY_ID
        from                        APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY  
        where SNAPSHOT_DATE <= (current_date() -7)
          and (    CLOSE_BOOKINGS_DATE > (current_date() -14)
                OR CLOSE_BOOKINGS_DATE is null)
        group by CRM_SOURCE
             , OPPORTUNITY_ID              
)
select B.ACCOUNT_NAME
     , B.OPPORTUNITY_NAME 
     , A.CRM_SOURCE
     , A.OPPORTUNITY_ID
     , B.C2_STAGE
     , B.FORECAST_CATEGORY
     , B.AVERAGE_ACV
     , B.FIRST_YEARS_BILLINGS
     , B.CREATED_DATE
     , B.CLOSE_BOOKINGS_DATE
     , B.ESTIMATED_CLOSE_DATE
     , B.SNAPSHOT_DATE  
from                        max_last_week A
inner join                  APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY B
             ON  A.CRM_SOURCE = B.CRM_SOURCE
             AND A.OPPORTUNITY_ID = B.OPPORTUNITY_ID
             AND A.MAX_SNAP = B.SNAPSHOT_DATE 
order by B.ACCOUNT_NAME, A.OPPORTUNITY_ID 
;


-- this could be a view definition that would align on the end of a weekley period, and report on open opps during the current Fiscal quarter
with current_period as (
        select "Fiscal Period"
             , "Week (Quarter)"  
        from "APTTUS_DW"."SF_PRODUCTION"."Dates" 
        where "Date" = current_date() 
)
, current_week as (
        select MAX(D."Date") as "Current_WeekEndDate"
             , MIN(D."Date") as "Current_WeekStartDate"       
        from         "APTTUS_DW"."SF_PRODUCTION"."Dates" D
        inner join   current_period C
                   ON  D."Fiscal Period" = C."Fiscal Period"
                   AND D."Week (Quarter)" = C."Week (Quarter)"      
)
, current_quarter as (
        select MIN(D."Date") as "Current_FPStart"      
             , MAX(D."Date") as "Current_FPEnd"  
        from         "APTTUS_DW"."SF_PRODUCTION"."Dates" D
        inner join   current_period C
                   ON  D."Fiscal Period" = C."Fiscal Period"       
)
, relevant_periods as (
        select MIN("Date") AS PeriodStartDate
             , MAX("Date") AS PeriodEndDate
             , "Fiscal Period"
             , "Week (Quarter)" 
        from "APTTUS_DW"."SF_PRODUCTION"."Dates" 
        where "Date" >= (Select "Current_FPStart" from current_quarter)
          and "Date" <= (Select "Current_WeekEndDate" from current_week)  
        group by "Fiscal Period", "Week (Quarter)"  
)        
, max_per_period as (
        select MAX(A.SNAPSHOT_DATE) as PERIOD_MAX_SNAP
             , A.CRM_SOURCE
             , A.OPPORTUNITY_ID
             , B."Fiscal Period" 
             , B."Week (Quarter)"
        from                        APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY A
        inner join                  relevant_periods B
                       on 1=1 -- yes the cartesian product is "on purpose"
        where A.SNAPSHOT_DATE <= B.PeriodEndDate
          and A.CREATED_DATE <= B.PeriodStartDate
          and (A.CLOSE_BOOKINGS_DATE is null 
                OR A.CLOSE_BOOKINGS_DATE between (Select "Current_FPStart" from current_quarter) AND (Select "Current_FPEnd" from current_quarter)
              )     
        group by A.CRM_SOURCE
             , A.OPPORTUNITY_ID
             , B."Fiscal Period" 
             , B."Week (Quarter)"               
)
select B.ACCOUNT_NAME
     , B.OPPORTUNITY_NAME 
     , A.CRM_SOURCE
     , A.OPPORTUNITY_ID
     , A."Fiscal Period" 
     , A."Week (Quarter)"
     , B.C2_STAGE
     , B.FORECAST_CATEGORY
     , B.AVERAGE_ACV
     , B.FIRST_YEARS_BILLINGS
     , B.CREATED_DATE
     , B.CLOSE_BOOKINGS_DATE
     , B.ESTIMATED_CLOSE_DATE
     , B.SNAPSHOT_DATE  
from                        max_per_period A
inner join                  APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY B
             ON  A.CRM_SOURCE = B.CRM_SOURCE
             AND A.OPPORTUNITY_ID = B.OPPORTUNITY_ID
             AND A.PERIOD_MAX_SNAP = B.SNAPSHOT_DATE 
order by B.ACCOUNT_NAME, A.OPPORTUNITY_ID, A."Week (Quarter)"
;
