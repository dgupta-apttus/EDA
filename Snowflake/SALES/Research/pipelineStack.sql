-- APTTUS_DW.SF_PRODUCTION.SALES_PIPELINESNAPSHOT source

create view SALES_PIPELINESNAPSHOT 
comment='Sales Pipeline Snapshot view buit on Snapshot data. Use for Managment Reporting 
---V1 05/15/2020 ---Naveen
---V2 05/21/2020 ---Naveen ---Added Type column
' 
as 
with ctesnapshotdate as (
select min("Date")PeriodStartDate, MAX("Date") PeriodEndDate, "Fiscal Period", "Week (Quarter)"  from "APTTUS_DW"."SF_PRODUCTION"."Dates" 
where "Date" >='2020-02-01'
group by "Fiscal Period", "Week (Quarter)"
order by Min("Date")
),
cteOpps as (
select 
--count(1)
--"Created Date", 
"Opportunity Name", "Opportunity ID", "Account ID", "Account Name", "Products of Interest", "Sales Play", "Geo"
from "APTTUS_DW"."SF_PRODUCTION"."Opportunity_Sales"
where
("Opportunity Name" not like '%TEST%' or "Opportunity Name" not like '%Test%' or "Opportunity Name" not like '%test%' or "Opportunity Name" not like 'Test%')
and ("Account Name" not like '%TEST%' or "Account Name" not like '%Test%' or "Account Name" not like '%test%' or "Account Name" not like 'Test%')
--and ("Account Owner Name" <> 'CRM Admin'and "Opportunity Owner" <> 'CRM Admin')
and "Created Date" >= '2018-01-01'
order by 1
--limit 10
)
select 
S."Opportunity ID"
, S."Account ID"
, S. "Account Name"
, 'https://apttus2.lightning.force.com/lightning/r/Opportunity/' || S."Opportunity ID" || '/view' OppurtunityURL
, 'https://apttus2.lightning.force.com/lightning/r/Account/' || S. "Account ID" || '/view' AccountURL
, S."Products of Interest" "Latst.ProdInterest"
, S."Sales Play" "Latest.SalesPlay"
, O."Opportunity Name"
, O."Product of Interest"
, O."Sales Play"
, O."Account Owner Name"
, O."Opportunity Owner"
, O."CreatedDate"
, O."Won_Lost_Date__c"
, O."Stage"
, O."Region"
, O."Sub Region"
, O."Forecast Category"
, O."Highest_Achieved_Stage__c"
, (O."Snapshot Date" - O."CreatedDate")/30.0 as "Opp.Age(Month)"
, (O."Snapshot Date" - O."Next Step Last Edited")/30.0 as "NextStep.Age(Month)"

, CASE
    WHEN O."Region" = 'APAC' THEN 'APAC'
    WHEN O."Region" in ( 'DACH', 'DACH & Nordics', 'North & South EMEA', 'Southern Europe' )  THEN 'EMEA'    
    WHEN O."Region" IN('North America Commercial', 'North America Enterprise',  'Strategic Accounts' , 'N/A', 'HLS') THEN	'AMER'   
    ELSE IFNULL(S."Geo", 'AMER')
 END "Geo"
 , IFNULL(O."First Year ACV (USD)",O."ACV (USD)" ) as "Calc.ACV"
,CASE 
    WHEN O."Stage" ='Closed Lost' AND O."Won_Lost_Date__c" IS NULL THEN NULL
    WHEN O."Stage" ='Closed Lost' AND O."Won_Lost_Date__c" IS NOT NULL THEN O."Won_Lost_Date__c"
ELSE O."Close Date"
END CloseDate_Adj
, O."Close Date"
, O."Snapshot Date"
, datesnapshot.PeriodStartDate 
, datesnapshot.PeriodEndDate
, dateclose."Fiscal Period" CloseFP
, dateclose."Week (Quarter)" CloseFW
, datesnapshot."Fiscal Period" SnapFP
, datesnapshot."Week (Quarter)" SnapFW
, O."Next Steps"
, O."Type"
from "APTTUS_DW"."SF_PRODUCTION"."Snapshot_Data_Table" O
LEFT JOIN ctesnapshotdate datesnapshot on datesnapshot.PeriodStartDate = O."Snapshot Date" 
LEFT JOIN "APTTUS_DW"."SF_PRODUCTION"."Dates" dateclose ON dateclose."Date" = O."Close Date" 
LEFT JOIN cteOpps S on S."Opportunity Name" = O."Opportunity Name" --(8065)
where 
O."Stage" in ('3 - Justification','2 - Validation','1 - Discovery','4 - Negotiation')
and O."Type" in ('Add-on Subscription','New Business - New Logo','New Business - New Operating Division','New Business - New Product')
and (O."Opportunity Name" not like '%TEST%' or O."Opportunity Name" not like '%Test%' or O."Opportunity Name" not like '%test%' or O."Opportunity Name" not like 'Test%')
and (O."Account Name" not like '%TEST%' or O."Account Name" not like '%Test%' or O."Account Name" not like '%test%' or O."Account Name" not like 'Test%')
and (O."Account Owner Name" <> 'CRM Admin'and O."Opportunity Owner" <> 'CRM Admin')
and O."Account Owner Name"  <> 'Abhirup Dutta'
and SNAPFP is not null 
--and S."Opportunity ID" IS NOT NULL
--and O."Opportunity Name" Like '%CLM for WNS%'
order by SNAPFP ASC, SNAPFW ASC;


-- APTTUS_DW.SF_PRODUCTION.SALES_PIPELINESNAPSHOT_HISTORY source

create OR REPLACE view SALES_PIPELINESNAPSHOT_HISTORY 
comment='Sales Pipeline Snapshot view buit on Snapshot data. Use for Managment Reporting 
---V1 05/15/2020 ---Naveen
---V2 05/21/2020 ---Naveen ---Added Type column
---V3 07/07/2020 ---Naveen \ Caitlin ---Union Historical Data
' 
as 
with ctesnapshotdate as (
select min("Date")PeriodStartDate, MAX("Date") PeriodEndDate, "Fiscal Period", "Week (Quarter)"  from "APTTUS_DW"."SF_PRODUCTION"."Dates" 
where "Date" >='2019-02-01'
group by "Fiscal Period", "Week (Quarter)"
order by Min("Date")
),
cteOpps as (
select 
--count(1)
--"Created Date", 
"Opportunity Name", "Opportunity ID", "Account ID", "Account Name", "Products of Interest", "Sales Play", "Geo"
from "APTTUS_DW"."SF_PRODUCTION"."Opportunity_Sales"
where
("Opportunity Name" not like '%TEST%' or "Opportunity Name" not like '%Test%' or "Opportunity Name" not like '%test%' or "Opportunity Name" not like 'Test%')
and ("Account Name" not like '%TEST%' or "Account Name" not like '%Test%' or "Account Name" not like '%test%' or "Account Name" not like 'Test%')
--and ("Account Owner Name" <> 'CRM Admin'and "Opportunity Owner" <> 'CRM Admin')
and "Created Date" >= '2018-01-01'
--order by 1
--limit 10
),
ctePipelineHist AS (
select 
S."Opportunity ID"
, S."Account ID"
, S. "Account Name"
, 'https://apttus2.lightning.force.com/lightning/r/Opportunity/' || S."Opportunity ID" || '/view' OppurtunityURL
, 'https://apttus2.lightning.force.com/lightning/r/Account/' || S. "Account ID" || '/view' AccountURL
, S."Products of Interest" "Latst.ProdInterest"
, S."Sales Play" "Latest.SalesPlay"
, O."Opportunity Name"
, O."Product of Interest"
, O."Sales Play"
, O."Account Owner Name"
, O."Opportunity Owner"
, O."CreatedDate"
, O."Won_Lost_Date__c"
, O."Stage"
, O."Region"
, O."Sub Region"
, O."Forecast Category"
, O."Highest_Achieved_Stage__c"
, (O."Snapshot Date" - O."CreatedDate")/30.0 as "Opp.Age(Month)"
, NULL as "NextStep.Age(Month)"
, CASE
    WHEN O."Region" = 'APAC' THEN 'APAC'
    WHEN O."Region" in ( 'DACH', 'DACH & Nordics', 'North & South EMEA', 'Southern Europe' )  THEN 'EMEA'    
    WHEN O."Region" IN('North America Commercial', 'North America Enterprise',  'Strategic Accounts' , 'N/A', 'HLS') THEN	'AMER'   
    ELSE IFNULL(S."Geo", 'AMER')
 END "Geo"
 , IFNULL(O."First Year ACV (USD)",O."ACV (USD)" ) as "Calc.ACV"
,CASE 
    WHEN O."Stage" ='Closed Lost' AND O."Won_Lost_Date__c" IS NULL THEN NULL
    WHEN O."Stage" ='Closed Lost' AND O."Won_Lost_Date__c" IS NOT NULL THEN O."Won_Lost_Date__c"
ELSE O."Close Date"
END CloseDate_Adj
, O."Close Date"
, O."Snapshot Date"
, datesnapshot.PeriodStartDate 
, datesnapshot.PeriodEndDate
, dateclose."Fiscal Period" CloseFP
, dateclose."Week (Quarter)" CloseFW
, datesnapshot."Fiscal Period" SnapFP
, datesnapshot."Week (Quarter)" SnapFW
, NULL AS "Next Steps"
, O."Type"
, 'History' AS "SNASHOTTYPE"
from APTTUS_DW.SF_PRODUCTION."SFDC_Lightning_Snapshot_Data" O
LEFT JOIN ctesnapshotdate datesnapshot on datesnapshot.PeriodStartDate = O."Snapshot Date" 
LEFT JOIN "APTTUS_DW"."SF_PRODUCTION"."Dates" dateclose ON dateclose."Date" = O."Close Date" 
LEFT JOIN cteOpps S on S."Opportunity Name" = O."Opportunity Name" --(8065)
where 
O."Stage" in ('3 - Justification','2 - Validation','1 - Discovery','4 - Negotiation')
and O."Type" in ('Add-on Subscription','New Business - New Logo','New Business - New Operating Division','New Business - New Product')
and (O."Opportunity Name" not like '%TEST%' or O."Opportunity Name" not like '%Test%' or O."Opportunity Name" not like '%test%' or O."Opportunity Name" not like 'Test%')
and (O."Account Name" not like '%TEST%' or O."Account Name" not like '%Test%' or O."Account Name" not like '%test%' or O."Account Name" not like 'Test%')
and (O."Account Owner Name" <> 'CRM Admin'and O."Opportunity Owner" <> 'CRM Admin')
and O."Account Owner Name"  <> 'Abhirup Dutta'
and SNAPFP is not null 
)
SELECT * FROM ctePipelineHist;


-- APTTUS_DW.SF_PRODUCTION."Snapshot_Data_Table" source

CREATE OR REPLACE VIEW "Snapshot_Data_Table" 
AS
SELECT 
"Account Name",
"Account Owner Name",
"Account Region",
"Account Sub Region",
"Account Vertical",
"Account.BillingCountry",
"Account.BillingState",
"Account.CreatedDate",
"Account.Industry",
To_Decimal("ACV (USD)")"ACV (USD)",
"Close Date",
"Conversion Rate",
"CreatedDate",
To_Decimal("First Year ACV (USD)")"First Year ACV (USD)",
"Forecast Category",
"ForecastCategory",
"FY20 Segment",
"Highest_Achieved_Stage__c",
"LeadSource",
"Loss_Reason__c",
"Lost Master Date",
"Manager__c",
"Mintigo - Predictive CLM Rank",
"Mintigo - Predictive CLM Score",
"Mintigo - Predictive CPQ Rank",
"Mintigo - Predictive CPQ Score",
"Mintigo - Predictive QTC Rank",
"Mintigo - Predictive QTC Score",
"Opportunity Name",
"Opportunity Owner",
"Opportunity_Source__c",
"Platform",
"Product of Interest",
To_Decimal("Ramped ACV (USD)") "Ramped ACV (USD)",
"Region",
"Sales Play",
"SE Name",
"Snapshot Date",
"Stage",
"Sub Region",
To_Decimal("TCV (USD)") "TCV (USD)",
"Type",
"Weighted ACV (USD)",
"Win_Reason__c",
"Won_Lost_Date__c",
"X18_Digit_Old_SFDC_ID__c",
"Next Steps",
"Next Step Last Edited",
DAYOFWEEK("Snapshot Date") "DayOfWeek"
,CONCAT(MONTHNAME("Snapshot Date"), ' - ' ,TO_VARCHAR(DAY("Snapshot Date")))"DateName"
FROM "APTTUS_DW"."SF_PRODUCTION"."SFDC_Lightning_Snapshot_Data_FY21";

SHOW tasks IN SF_PRODUCTION;

-- APTTUS_DW.SF_PRODUCTION."Opportunity_Sales" source

CREATE OR REPLACE VIEW "Opportunity_Sales"
AS
SELECT * 
,CONCAT('Week ',TO_CHAR("Close Week (Year) OrderBy"))  "Close Week (Year)"
,CONCAT('Week ',TO_CHAR("Close Week OrderBy")) "Close Week"
,CONCAT('Week ',TO_CHAR("Created Week (Year) OrderBy"))  "Created Week (Year)"
,CONCAT('Week ',TO_CHAR("Created Week OrderBy")) "Created Week"
,CONCAT('Q',TO_CHAR("FiscalQuarter"),' Week ' ,TO_CHAR("Close Week OrderBy")) "Week Closed per Quarter"
,CASE "Stage"
WHEN '0 - Qualification' THEN 1
WHEN '1 - Discovery' THEN 2
WHEN '2 - Validation' THEN 3
WHEN '3 - Justification' THEN 4
WHEN '4 - Negotiation' THEN 5
WHEN 'Pending Closed Won' THEN 6
WHEN 'Closed Won' THEN 7
WHEN 'Closed Lost' THEN 8
ELSE 100 END "Stage Order By"
FROM
(
SELECT 
  
"Account ID"
,"Account Name"
,"Age of Deal (Days)"
,"Age of Deal (Months)"
,"SE Name"
,"BDR Assigned"
,"Campaign ID"
,"Close Date"
,"Contact"
,"Created By"
,"Created Date"
,"Currency"
,"Description"
,"Dynamics GUID"
,"First Year Billings"
,"Forecast Category"
  , "Geo"
,"Highest Achieved Stage"
,"Opportunity ID"
,"Inbound/Outbound"
,"Last Activity Date"
,"Last Modified By"
,"Last Modified Date"
,"Lead Source - Global"
,"Loss Reason"
,"Loss Sub Reason"
,"Opportunity Name"
,"Next Steps"
,"Next Step Last Edited"
,"Opportunity Accepted"
,"Opportunity Accepted Date"
,"Opportunity Source"
,"Opportunity Owner"
,"Partner Source"
,"Platform"
,"Primary Competitor"
,"Primary Quote ID"
,"Probability"
,"Products of Interest"
,"Professional Judgement"
,"SI Partner"
, "Segment"
,"Purchase Order Number"
,"Ramped ACV"
,"Region"
,"Sales Play"
,"SI Partner Notes"
,"Stage"
,"Sub Region"
,"Term (Months)"
,"Total Deal Value"
,"Total Renewal Due"
,"Type"
,"Valid Until Date"
,"Win Loss Detail Notes"
,"Win Reason"
,"Old SFDC ID"
,"ACV (USD)"
,"FiscalQuarter"
,"Sales Territory"
,"Geography"
,CASE WHEN "Stage" = 'Closed Won' THEN "ACV (USD)" ELSE 0 END "ACV (USD) Booked"
,CASE WHEN "Stage" in ('1 - Discovery', '2 - Validation', '3 - Justification', '4 - Negotiation') THEN "ACV (USD)" ELSE 0 END "ACV (USD) In Pipeline"
,MonthName("Close Date") "Close Month"
,MonthName("Created Date") "Created Month"
,Month("Close Date") "Close Month OrderBy"
,Month("Created Date") "Created Month OrderBy"

,Concat('FY','',TO_CHAR(CASE WHEN MONTH("Close Date") = 1 then RIGHT(TO_CHAR(YEAR("Close Date")),2) ELSE RIGHT(TO_CHAR(YEAR("Close Date") +1),2) END), 
        '-' , 'Q',TO_CHAR("FiscalQuarter")) "Fiscal Period"
,CASE WHEN MONTH("Close Date") = 1 then (YEAR("Close Date")*1000)+"FiscalQuarter" 
  ELSE ((YEAR("Close Date")+1)*1000)+"FiscalQuarter" END "Fiscal Period OrderBy"

,Concat('FY','',TO_CHAR(CASE WHEN MONTH("Created Date") = 1 then RIGHT(TO_CHAR(YEAR("Created Date")),2) ELSE RIGHT(TO_CHAR(YEAR("Created Date") +1),2) END), 
        '-' , 'Q',TO_CHAR("FiscalQuarterCreatedDate")) "Fiscal Period Created Date"
,CASE WHEN MONTH("Created Date") = 1 then (YEAR("Created Date")*1000)+"FiscalQuarterCreatedDate" 
  ELSE ((YEAR("Created Date")+1)*1000)+"FiscalQuarterCreatedDate" END "Fiscal Period CreatedDate OrderBy"
  
,CASE WHEN "Type" in ('New Business - New Product', 'New Business - New Operating Division', 'Add-on Subscription') then 'Expansion' 
WHEN "Type" = 'New Business - New Logo' then 'New Logo' ELSE 'Other 'END "New Logo vs Expansion"

,CASE
when MONTH("Close Date") = 2 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Close Date") = 2 AND DAY("Close Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Close Date") = 2 AND DAY("Close Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Close Date") = 2 AND DAY("Close Date") BETWEEN 22 AND 29 THEN  4
when MONTH("Close Date") = 3 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  5
when MONTH("Close Date") = 3 AND DAY("Close Date") BETWEEN 8 AND 14 THEN  6
when MONTH("Close Date") = 3 AND DAY("Close Date") BETWEEN 15 AND 21 THEN  7
when MONTH("Close Date") = 3 AND DAY("Close Date") BETWEEN 22 AND 28 THEN  8
when MONTH("Close Date") = 3 AND DAY("Close Date") > 28 THEN  9
when MONTH("Close Date") = 4 AND DAY("Close Date") BETWEEN 1 AND 4 THEN  9
when MONTH("Close Date") = 4 AND DAY("Close Date") BETWEEN 5 AND 11 THEN  10
when MONTH("Close Date") = 4 AND DAY("Close Date") BETWEEN 12 AND 18 THEN  11
when MONTH("Close Date") = 4 AND DAY("Close Date") BETWEEN 19 AND 25 THEN  12
when MONTH("Close Date") = 4 AND DAY("Close Date") > 25 THEN 13
when MONTH("Close Date") = 5 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Close Date") = 5 AND DAY("Close Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Close Date") = 5 AND DAY("Close Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Close Date") = 5 AND DAY("Close Date") BETWEEN 22 AND 28 THEN  4
when MONTH("Close Date") = 5 AND DAY("Close Date") > 28 THEN  5
when MONTH("Close Date") = 6 AND DAY("Close Date") BETWEEN 1 AND 4 THEN  5
when MONTH("Close Date") = 6 AND DAY("Close Date") BETWEEN 5 AND 11 THEN  6
when MONTH("Close Date") = 6 AND DAY("Close Date") BETWEEN 12 AND 18 THEN  7
when MONTH("Close Date") = 6 AND DAY("Close Date") BETWEEN 19 AND 25 THEN  8
when MONTH("Close Date") = 6 AND DAY("Close Date") > 25 THEN 9
when MONTH("Close Date") = 7 AND DAY("Close Date") BETWEEN 1 AND 2 THEN  9
when MONTH("Close Date") = 7 AND DAY("Close Date") BETWEEN 3 AND 9 THEN  10
when MONTH("Close Date") = 7 AND DAY("Close Date") BETWEEN 10 AND 16 THEN  11
when MONTH("Close Date") = 7 AND DAY("Close Date") BETWEEN 17 AND 23 THEN  12
when MONTH("Close Date") = 7 AND DAY("Close Date") > 23 THEN 13
when MONTH("Close Date") = 8 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Close Date") = 8 AND DAY("Close Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Close Date") = 8 AND DAY("Close Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Close Date") = 8 AND DAY("Close Date") BETWEEN 22 AND 28 THEN  4
when MONTH("Close Date") = 8 AND DAY("Close Date") > 28 THEN  5
when MONTH("Close Date") = 9 AND DAY("Close Date") BETWEEN 1 AND 4 THEN  5
when MONTH("Close Date") = 9 AND DAY("Close Date") BETWEEN 5 AND 11 THEN  6
when MONTH("Close Date") = 9 AND DAY("Close Date") BETWEEN 12 AND 18 THEN  7
when MONTH("Close Date") = 9 AND DAY("Close Date") BETWEEN 19 AND 25 THEN  8
when MONTH("Close Date") = 9 AND DAY("Close Date") > 25 THEN 9
when MONTH("Close Date") = 10 AND DAY("Close Date") BETWEEN 1 AND 2 THEN  9
when MONTH("Close Date") = 10 AND DAY("Close Date") BETWEEN 3 AND 9 THEN  10
when MONTH("Close Date") = 10 AND DAY("Close Date") BETWEEN 10 AND 16 THEN  11
when MONTH("Close Date") = 10 AND DAY("Close Date") BETWEEN 17 AND 23 THEN  12
when MONTH("Close Date") = 10 AND DAY("Close Date") > 23 THEN 13
when MONTH("Close Date") = 11 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Close Date") = 11 AND DAY("Close Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Close Date") = 11 AND DAY("Close Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Close Date") = 11 AND DAY("Close Date") BETWEEN 22 AND 28 THEN  4
when MONTH("Close Date") = 11 AND DAY("Close Date") > 28 THEN  5
when MONTH("Close Date") = 12 AND DAY("Close Date") BETWEEN 1 AND 5 THEN  5
when MONTH("Close Date") = 12 AND DAY("Close Date") BETWEEN 6 AND 12 THEN  6
when MONTH("Close Date") = 12 AND DAY("Close Date") BETWEEN 13 AND 19 THEN  7
when MONTH("Close Date") = 12 AND DAY("Close Date") BETWEEN 20 AND 26 THEN  8
when MONTH("Close Date") = 12 AND DAY("Close Date") > 26 THEN 9
when MONTH("Close Date") = 1 AND DAY("Close Date") BETWEEN 1 AND 2 THEN  9
when MONTH("Close Date") = 1 AND DAY("Close Date") BETWEEN 3 AND 9 THEN  10
when MONTH("Close Date") = 1 AND DAY("Close Date") BETWEEN 10 AND 16 THEN  11
when MONTH("Close Date") = 1 AND DAY("Close Date") BETWEEN 17 AND 23 THEN  12
when MONTH("Close Date") = 1 AND DAY("Close Date") > 23 THEN 13
ELSE 0 END "Close Week OrderBy"

,CASE
when MONTH("Created Date") = 2 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Created Date") = 2 AND DAY("Created Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Created Date") = 2 AND DAY("Created Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Created Date") = 2 AND DAY("Created Date") BETWEEN 22 AND 29 THEN  4
when MONTH("Created Date") = 3 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  5
when MONTH("Created Date") = 3 AND DAY("Created Date") BETWEEN 8 AND 14 THEN  6
when MONTH("Created Date") = 3 AND DAY("Created Date") BETWEEN 15 AND 21 THEN  7
when MONTH("Created Date") = 3 AND DAY("Created Date") BETWEEN 22 AND 28 THEN  8
when MONTH("Created Date") = 3 AND DAY("Created Date") > 28 THEN  9
when MONTH("Created Date") = 4 AND DAY("Created Date") BETWEEN 1 AND 4 THEN  9
when MONTH("Created Date") = 4 AND DAY("Created Date") BETWEEN 5 AND 11 THEN  10
when MONTH("Created Date") = 4 AND DAY("Created Date") BETWEEN 12 AND 18 THEN  11
when MONTH("Created Date") = 4 AND DAY("Created Date") BETWEEN 19 AND 25 THEN  12
when MONTH("Created Date") = 4 AND DAY("Created Date") > 25 THEN 13
when MONTH("Created Date") = 5 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Created Date") = 5 AND DAY("Created Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Created Date") = 5 AND DAY("Created Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Created Date") = 5 AND DAY("Created Date") BETWEEN 22 AND 28 THEN  4
when MONTH("Created Date") = 5 AND DAY("Created Date") > 28 THEN  5
when MONTH("Created Date") = 6 AND DAY("Created Date") BETWEEN 1 AND 4 THEN  5
when MONTH("Created Date") = 6 AND DAY("Created Date") BETWEEN 5 AND 11 THEN  6
when MONTH("Created Date") = 6 AND DAY("Created Date") BETWEEN 12 AND 18 THEN  7
when MONTH("Created Date") = 6 AND DAY("Created Date") BETWEEN 19 AND 25 THEN  8
when MONTH("Created Date") = 6 AND DAY("Created Date") > 25 THEN 9
when MONTH("Created Date") = 7 AND DAY("Created Date") BETWEEN 1 AND 2 THEN  9
when MONTH("Created Date") = 7 AND DAY("Created Date") BETWEEN 3 AND 9 THEN  10
when MONTH("Created Date") = 7 AND DAY("Created Date") BETWEEN 10 AND 16 THEN  11
when MONTH("Created Date") = 7 AND DAY("Created Date") BETWEEN 17 AND 23 THEN  12
when MONTH("Created Date") = 7 AND DAY("Created Date") > 23 THEN 13
when MONTH("Created Date") = 8 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Created Date") = 8 AND DAY("Created Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Created Date") = 8 AND DAY("Created Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Created Date") = 8 AND DAY("Created Date") BETWEEN 22 AND 28 THEN  4
when MONTH("Created Date") = 8 AND DAY("Created Date") > 28 THEN  5
when MONTH("Created Date") = 9 AND DAY("Created Date") BETWEEN 1 AND 4 THEN  5
when MONTH("Created Date") = 9 AND DAY("Created Date") BETWEEN 5 AND 11 THEN  6
when MONTH("Created Date") = 9 AND DAY("Created Date") BETWEEN 12 AND 18 THEN  7
when MONTH("Created Date") = 9 AND DAY("Created Date") BETWEEN 19 AND 25 THEN  8
when MONTH("Created Date") = 9 AND DAY("Created Date") > 25 THEN 9
when MONTH("Created Date") = 10 AND DAY("Created Date") BETWEEN 1 AND 2 THEN  9
when MONTH("Created Date") = 10 AND DAY("Created Date") BETWEEN 3 AND 9 THEN  10
when MONTH("Created Date") = 10 AND DAY("Created Date") BETWEEN 10 AND 16 THEN  11
when MONTH("Created Date") = 10 AND DAY("Created Date") BETWEEN 17 AND 23 THEN  12
when MONTH("Created Date") = 10 AND DAY("Created Date") > 23 THEN 13
when MONTH("Created Date") = 11 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Created Date") = 11 AND DAY("Created Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Created Date") = 11 AND DAY("Created Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Created Date") = 11 AND DAY("Created Date") BETWEEN 22 AND 28 THEN  4
when MONTH("Created Date") = 11 AND DAY("Created Date") > 28 THEN  5
when MONTH("Created Date") = 12 AND DAY("Created Date") BETWEEN 1 AND 5 THEN  5
when MONTH("Created Date") = 12 AND DAY("Created Date") BETWEEN 6 AND 12 THEN  6
when MONTH("Created Date") = 12 AND DAY("Created Date") BETWEEN 13 AND 19 THEN  7
when MONTH("Created Date") = 12 AND DAY("Created Date") BETWEEN 20 AND 26 THEN  8
when MONTH("Created Date") = 12 AND DAY("Created Date") > 26 THEN 9
when MONTH("Created Date") = 1 AND DAY("Created Date") BETWEEN 1 AND 2 THEN  9
when MONTH("Created Date") = 1 AND DAY("Created Date") BETWEEN 3 AND 9 THEN  10
when MONTH("Created Date") = 1 AND DAY("Created Date") BETWEEN 10 AND 16 THEN  11
when MONTH("Created Date") = 1 AND DAY("Created Date") BETWEEN 17 AND 23 THEN  12
when MONTH("Created Date") = 1 AND DAY("Created Date") > 23 THEN 13
ELSE 0 END "Created Week OrderBy"

,CASE
when MONTH("Close Date") = 2 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Close Date") = 2 AND DAY("Close Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Close Date") = 2 AND DAY("Close Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Close Date") = 2 AND DAY("Close Date") BETWEEN 22 AND 29 THEN  4
when MONTH("Close Date") = 3 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  5
when MONTH("Close Date") = 3 AND DAY("Close Date") BETWEEN 8 AND 14 THEN  6
when MONTH("Close Date") = 3 AND DAY("Close Date") BETWEEN 15 AND 21 THEN  7
when MONTH("Close Date") = 3 AND DAY("Close Date") BETWEEN 22 AND 28 THEN  8
when MONTH("Close Date") = 3 AND DAY("Close Date") > 28 THEN  9
when MONTH("Close Date") = 4 AND DAY("Close Date") BETWEEN 1 AND 4 THEN  9
when MONTH("Close Date") = 4 AND DAY("Close Date") BETWEEN 5 AND 11 THEN  10
when MONTH("Close Date") = 4 AND DAY("Close Date") BETWEEN 12 AND 18 THEN  11
when MONTH("Close Date") = 4 AND DAY("Close Date") BETWEEN 19 AND 25 THEN  12
when MONTH("Close Date") = 4 AND DAY("Close Date") > 25 THEN 13
when MONTH("Close Date") = 5 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  14
when MONTH("Close Date") = 5 AND DAY("Close Date") BETWEEN 8 AND 14 THEN 15
when MONTH("Close Date") = 5 AND DAY("Close Date") BETWEEN 15 AND 21 THEN 16
when MONTH("Close Date") = 5 AND DAY("Close Date") BETWEEN 22 AND 28 THEN  17
when MONTH("Close Date") = 5 AND DAY("Close Date") > 28 THEN  18
when MONTH("Close Date") = 6 AND DAY("Close Date") BETWEEN 1 AND 4 THEN  18
when MONTH("Close Date") = 6 AND DAY("Close Date") BETWEEN 5 AND 11 THEN  19
when MONTH("Close Date") = 6 AND DAY("Close Date") BETWEEN 12 AND 18 THEN  20
when MONTH("Close Date") = 6 AND DAY("Close Date") BETWEEN 19 AND 25 THEN  21
when MONTH("Close Date") = 6 AND DAY("Close Date") > 25 THEN 22
when MONTH("Close Date") = 7 AND DAY("Close Date") BETWEEN 1 AND 2 THEN  22
when MONTH("Close Date") = 7 AND DAY("Close Date") BETWEEN 3 AND 9 THEN  23
when MONTH("Close Date") = 7 AND DAY("Close Date") BETWEEN 10 AND 16 THEN  24
when MONTH("Close Date") = 7 AND DAY("Close Date") BETWEEN 17 AND 23 THEN  25
when MONTH("Close Date") = 7 AND DAY("Close Date") > 23 THEN 26
when MONTH("Close Date") = 8 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  27
when MONTH("Close Date") = 8 AND DAY("Close Date") BETWEEN 8 AND 14 THEN  28
when MONTH("Close Date") = 8 AND DAY("Close Date") BETWEEN 15 AND 21 THEN  29
when MONTH("Close Date") = 8 AND DAY("Close Date") BETWEEN 22 AND 28 THEN  30
when MONTH("Close Date") = 8 AND DAY("Close Date") > 28 THEN  31
when MONTH("Close Date") = 9 AND DAY("Close Date") BETWEEN 1 AND 4 THEN  31
when MONTH("Close Date") = 9 AND DAY("Close Date") BETWEEN 5 AND 11 THEN  32
when MONTH("Close Date") = 9 AND DAY("Close Date") BETWEEN 12 AND 18 THEN  33
when MONTH("Close Date") = 9 AND DAY("Close Date") BETWEEN 19 AND 25 THEN  34
when MONTH("Close Date") = 9 AND DAY("Close Date") > 25 THEN 35
when MONTH("Close Date") = 10 AND DAY("Close Date") BETWEEN 1 AND 2 THEN  35
when MONTH("Close Date") = 10 AND DAY("Close Date") BETWEEN 3 AND 9 THEN  36
when MONTH("Close Date") = 10 AND DAY("Close Date") BETWEEN 10 AND 16 THEN  37
when MONTH("Close Date") = 10 AND DAY("Close Date") BETWEEN 17 AND 23 THEN  38
when MONTH("Close Date") = 10 AND DAY("Close Date") > 23 THEN 39
when MONTH("Close Date") = 11 AND DAY("Close Date") BETWEEN 1 AND 7 THEN  40
when MONTH("Close Date") = 11 AND DAY("Close Date") BETWEEN 8 AND 14 THEN  41
when MONTH("Close Date") = 11 AND DAY("Close Date") BETWEEN 15 AND 21 THEN 42
when MONTH("Close Date") = 11 AND DAY("Close Date") BETWEEN 22 AND 28 THEN 43
when MONTH("Close Date") = 11 AND DAY("Close Date") > 28 THEN  44
when MONTH("Close Date") = 12 AND DAY("Close Date") BETWEEN 1 AND 5 THEN  44
when MONTH("Close Date") = 12 AND DAY("Close Date") BETWEEN 6 AND 12 THEN  45
when MONTH("Close Date") = 12 AND DAY("Close Date") BETWEEN 13 AND 19 THEN  46
when MONTH("Close Date") = 12 AND DAY("Close Date") BETWEEN 20 AND 26 THEN  47
when MONTH("Close Date") = 12 AND DAY("Close Date") > 26 THEN 48
when MONTH("Close Date") = 1 AND DAY("Close Date") BETWEEN 1 AND 2 THEN  48
when MONTH("Close Date") = 1 AND DAY("Close Date") BETWEEN 3 AND 9 THEN  49
when MONTH("Close Date") = 1 AND DAY("Close Date") BETWEEN 10 AND 16 THEN  50
when MONTH("Close Date") = 1 AND DAY("Close Date") BETWEEN 17 AND 23 THEN  51
when MONTH("Close Date") = 1 AND DAY("Close Date") > 23 THEN 52
ELSE 0 END "Close Week (Year) OrderBy"

,CASE
when MONTH("Created Date") = 2 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  1
when MONTH("Created Date") = 2 AND DAY("Created Date") BETWEEN 8 AND 14 THEN  2
when MONTH("Created Date") = 2 AND DAY("Created Date") BETWEEN 15 AND 21 THEN  3
when MONTH("Created Date") = 2 AND DAY("Created Date") BETWEEN 22 AND 29 THEN  4
when MONTH("Created Date") = 3 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  5
when MONTH("Created Date") = 3 AND DAY("Created Date") BETWEEN 8 AND 14 THEN  6
when MONTH("Created Date") = 3 AND DAY("Created Date") BETWEEN 15 AND 21 THEN  7
when MONTH("Created Date") = 3 AND DAY("Created Date") BETWEEN 22 AND 28 THEN  8
when MONTH("Created Date") = 3 AND DAY("Created Date") > 28 THEN  9
when MONTH("Created Date") = 4 AND DAY("Created Date") BETWEEN 1 AND 4 THEN  9
when MONTH("Created Date") = 4 AND DAY("Created Date") BETWEEN 5 AND 11 THEN  10
when MONTH("Created Date") = 4 AND DAY("Created Date") BETWEEN 12 AND 18 THEN  11
when MONTH("Created Date") = 4 AND DAY("Created Date") BETWEEN 19 AND 25 THEN  12
when MONTH("Created Date") = 4 AND DAY("Created Date") > 25 THEN 13
when MONTH("Created Date") = 5 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  14
when MONTH("Created Date") = 5 AND DAY("Created Date") BETWEEN 8 AND 14 THEN 15
when MONTH("Created Date") = 5 AND DAY("Created Date") BETWEEN 15 AND 21 THEN 16
when MONTH("Created Date") = 5 AND DAY("Created Date") BETWEEN 22 AND 28 THEN  17
when MONTH("Created Date") = 5 AND DAY("Created Date") > 28 THEN  18
when MONTH("Created Date") = 6 AND DAY("Created Date") BETWEEN 1 AND 4 THEN  18
when MONTH("Created Date") = 6 AND DAY("Created Date") BETWEEN 5 AND 11 THEN  19
when MONTH("Created Date") = 6 AND DAY("Created Date") BETWEEN 12 AND 18 THEN  20
when MONTH("Created Date") = 6 AND DAY("Created Date") BETWEEN 19 AND 25 THEN  21
when MONTH("Created Date") = 6 AND DAY("Created Date") > 25 THEN 22
when MONTH("Created Date") = 7 AND DAY("Created Date") BETWEEN 1 AND 2 THEN  22
when MONTH("Created Date") = 7 AND DAY("Created Date") BETWEEN 3 AND 9 THEN  23
when MONTH("Created Date") = 7 AND DAY("Created Date") BETWEEN 10 AND 16 THEN  24
when MONTH("Created Date") = 7 AND DAY("Created Date") BETWEEN 17 AND 23 THEN  25
when MONTH("Created Date") = 7 AND DAY("Created Date") > 23 THEN 26
when MONTH("Created Date") = 8 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  27
when MONTH("Created Date") = 8 AND DAY("Created Date") BETWEEN 8 AND 14 THEN  28
when MONTH("Created Date") = 8 AND DAY("Created Date") BETWEEN 15 AND 21 THEN  29
when MONTH("Created Date") = 8 AND DAY("Created Date") BETWEEN 22 AND 28 THEN  30
when MONTH("Created Date") = 8 AND DAY("Created Date") > 28 THEN  31
when MONTH("Created Date") = 9 AND DAY("Created Date") BETWEEN 1 AND 4 THEN  31
when MONTH("Created Date") = 9 AND DAY("Created Date") BETWEEN 5 AND 11 THEN  32
when MONTH("Created Date") = 9 AND DAY("Created Date") BETWEEN 12 AND 18 THEN  33
when MONTH("Created Date") = 9 AND DAY("Created Date") BETWEEN 19 AND 25 THEN  34
when MONTH("Created Date") = 9 AND DAY("Created Date") > 25 THEN 35
when MONTH("Created Date") = 10 AND DAY("Created Date") BETWEEN 1 AND 2 THEN  35
when MONTH("Created Date") = 10 AND DAY("Created Date") BETWEEN 3 AND 9 THEN  36
when MONTH("Created Date") = 10 AND DAY("Created Date") BETWEEN 10 AND 16 THEN  37
when MONTH("Created Date") = 10 AND DAY("Created Date") BETWEEN 17 AND 23 THEN  38
when MONTH("Created Date") = 10 AND DAY("Created Date") > 23 THEN 39
when MONTH("Created Date") = 11 AND DAY("Created Date") BETWEEN 1 AND 7 THEN  40
when MONTH("Created Date") = 11 AND DAY("Created Date") BETWEEN 8 AND 14 THEN  41
when MONTH("Created Date") = 11 AND DAY("Created Date") BETWEEN 15 AND 21 THEN 42
when MONTH("Created Date") = 11 AND DAY("Created Date") BETWEEN 22 AND 28 THEN 43
when MONTH("Created Date") = 11 AND DAY("Created Date") > 28 THEN  44
when MONTH("Created Date") = 12 AND DAY("Created Date") BETWEEN 1 AND 5 THEN  44
when MONTH("Created Date") = 12 AND DAY("Created Date") BETWEEN 6 AND 12 THEN  45
when MONTH("Created Date") = 12 AND DAY("Created Date") BETWEEN 13 AND 19 THEN  46
when MONTH("Created Date") = 12 AND DAY("Created Date") BETWEEN 20 AND 26 THEN  47
when MONTH("Created Date") = 12 AND DAY("Created Date") > 26 THEN 48
when MONTH("Created Date") = 1 AND DAY("Created Date") BETWEEN 1 AND 2 THEN  48
when MONTH("Created Date") = 1 AND DAY("Created Date") BETWEEN 3 AND 9 THEN  49
when MONTH("Created Date") = 1 AND DAY("Created Date") BETWEEN 10 AND 16 THEN  50
when MONTH("Created Date") = 1 AND DAY("Created Date") BETWEEN 17 AND 23 THEN  51
when MONTH("Created Date") = 1 AND DAY("Created Date") > 23 THEN 52
ELSE 0 END "Created Week (Year) OrderBy"

FROM 
(
SELECT 
ACCOUNTID AS "Account ID"
,A.NAME AS "Account Name"
,AGE_OF_THE_DEAL_DAYS__C AS "Age of Deal (Days)"
,(AGE_OF_THE_DEAL_DAYS__C/365)*12 AS "Age of Deal (Months)"
,SE.NAME AS "SE Name"
,UBDR.NAME AS "BDR Assigned"
,CAMPAIGNID AS "Campaign ID"
,TO_DATE(CLOSEDATE) AS "Close Date"
,CONTACT__C AS "Contact"
,UCB.NAME AS "Created By"
,TO_DATE(DATEADD(HOUR,-7,CREATEDDATE)) AS "Created Date"
,CURRENCYISOCODE AS "Currency"
,DESCRIPTION AS "Description"
,DYNAMICS_GUID__C AS "Dynamics GUID"
,IFNULL(FIRST_YEAR_ACV__C,0) AS "First Year Billings"
,FORECAST_CATEGORY__C AS "Forecast Category"
, "GEO__C" AS "Geo"
,HIGHEST_ACHIEVED_STAGE__C AS "Highest Achieved Stage"
,O.ID AS "Opportunity ID"
,INBOUND_OUTBOUND_OPPORTUNITY__C AS "Inbound/Outbound"
,LASTACTIVITYDATE AS "Last Activity Date"
,UMB.NAME AS "Last Modified By"
,LASTMODIFIEDDATE AS "Last Modified Date"
,LEAD_SOURCE_GLOBAL__C AS "Lead Source - Global"
,LOSS_REASON__C AS "Loss Reason"
,LOSS_SUB_CATEGORY__C AS "Loss Sub Reason"
--,MANAGER__C AS "Manager"
,O.NAME AS "Opportunity Name"
,NEXTSTEP AS "Next Steps"
,Next_Step_last_edited__c AS "Next Step Last Edited"
,OPPORTUNITY_ACCEPTED__C AS "Opportunity Accepted"
,OPPORTUNITY_ACCEPTED_DATE__C AS "Opportunity Accepted Date"
,OPPORTUNITY_SOURCE__C AS "Opportunity Source"
,UO.NAME AS "Opportunity Owner"
,PS.NAME AS "Partner Source"
,PLATFORM__C AS "Platform"
,PRIMARY_COMPETITOR__C AS "Primary Competitor"
,PRIMARY_QUOTE_ID__C AS "Primary Quote ID"
,PROBABILITY AS "Probability"
,PRODUCTS_OF_INTEREST__C AS "Products of Interest"
,PROFESSIONAL_JUDGEMENT__C AS "Professional Judgement"
,ASI.NAME AS "SI Partner"
,PURCHASE_ORDER_NUMBER__C AS "Purchase Order Number"
,RAMPED_ACV__C AS "Ramped ACV"
,REGION__C AS "Region"
,CASE WHEN LENGTH(IFNULL(SALES_PLAYS__C,''))>0 THEN  SALES_PLAYS__C ELSE 'Non-Core Sales Play' END  "Sales Play"
, "SEGMENT__C" AS "Segment"
,SI_PARTNER_NOTES__C AS "SI Partner Notes"
,STAGENAME AS "Stage"
--,SEGMENTATION__C AS "Segmentation"
,SUB_REGION__C AS "Sub Region"
,TERM_MONTHS__C AS "Term (Months)"
,IFNULL(TOTAL_DEAL_VALUE__C,0) AS "Total Deal Value"
,TOTAL_RENEWAL_DUE__C AS "Total Renewal Due"
,TYPE AS "Type"
,VALID_UNTIL_DATE__C AS "Valid Until Date"
,WIN_LOSS_DETAIL_NOTES__C AS "Win Loss Detail Notes"
,WIN_REASON__C AS "Win Reason"
,X18_DIGIT_OLD_SFDC_ID__C AS "Old SFDC ID"
,IFNULL(FIRST_YEAR_ACV__C,0)/FR."FX Rate" "ACV (USD)"
,CASE WHEN MONTH("Close Date") IN (2,3,4) THEN 1 
                WHEN MONTH("Close Date") IN (5,6,7) THEN 2 
                WHEN MONTH("Close Date") IN (8,9,10) THEN 3 
                ELSE 4 END "FiscalQuarter"
,CASE WHEN MONTH("Created Date") IN (2,3,4) THEN 1 
                WHEN MONTH("Created Date") IN (5,6,7) THEN 2 
                WHEN MONTH("Created Date") IN (8,9,10) THEN 3 
                ELSE 4 END "FiscalQuarterCreatedDate"
,(CASE
when Region__c='APAC' then 'APAC'
when Region__c IN ('DACH', 'DACH & Nordics') then 'DACH & Nordics'
when Region__c IN ('Southern Europe', 'North EMEA', 'North & South EMEA') then 'North & South EMEA'
when Region__c='Strategic Accounts' then 'Strategic Accounts'
when Region__c='North America Enterprise' AND SUB_REGION__C IN ('West', 'East', 'South', 'Central') then 'North America Enterprise [Sub Region]'
end) "Sales Territory"
,(CASE
when Region__c='APAC' then 'APAC'
when Region__c IN ('DACH', 'DACH & Nordics','North & South EMEA' ,'Southern Europe','North EMEA') then 'EMEA'
when Region__c IN ('North America Enterprise','Strategic Accounts','HLS','North America Commercial') then 'Americas'
else 'Other'
end) "Geography"
,CASE 
WHEN UPPER(O.NAME) LIKE '%TEST%' THEN 0
WHEN UPPER(O.NAME) LIKE '%CANCEL%' THEN 0
WHEN UPPER(O.NAME) LIKE '%REPLACE%' THEN 0
ELSE 1 END "Ignore_TestOpportunities"
FROM "APTTUS_DW"."SF_PRODUCTION"."OPPORTUNITY" O
LEFT OUTER JOIN "DOMO_SPREADSHEETS"."PUBLIC"."FX_RATE" FR ON O.CurrencyIsoCode = FR.Currency
LEFT OUTER JOIN (SELECT ID, NAME FROM "APTTUS_DW"."SF_PRODUCTION"."ACCOUNT") ASI ON O.PROJECTED_SI_PARTNER__C = ASI.ID
LEFT OUTER JOIN (SELECT ID, NAME FROM "APTTUS_DW"."SF_PRODUCTION"."ACCOUNT") PS ON O.PARTNER_SOURCE__C = PS.ID
LEFT OUTER JOIN (SELECT ID, NAME FROM "APTTUS_DW"."SF_PRODUCTION"."ACCOUNT") A ON O.ACCOUNTID = A.ID
LEFT OUTER JOIN (SELECT ID, NAME FROM "APTTUS_DW"."SF_PRODUCTION"."USER") UO ON O.OWNERID = UO.ID
LEFT OUTER JOIN (SELECT ID, NAME FROM "APTTUS_DW"."SF_PRODUCTION"."USER") SE ON O.APTTUS_SE__C = SE.ID
LEFT OUTER JOIN (SELECT ID, NAME FROM "APTTUS_DW"."SF_PRODUCTION"."USER") UBDR ON O.BDR_ASSIGNED_TO_OPPTY__C = UBDR.ID
LEFT OUTER JOIN (SELECT ID, NAME FROM "APTTUS_DW"."SF_PRODUCTION"."USER") UCB ON O.CREATEDBYID = UCB.ID
LEFT OUTER JOIN (SELECT ID, NAME FROM "APTTUS_DW"."SF_PRODUCTION"."USER") UMB ON O.LASTMODIFIEDBYID = UMB.ID
WHERE O.ISDELETED = FALSE --AND FIRST_YEAR_ACV__C >= 0 
) TBL WHERE "ACV (USD)" >= 0 AND "Ignore_TestOpportunities" = 1
)TBL2;


