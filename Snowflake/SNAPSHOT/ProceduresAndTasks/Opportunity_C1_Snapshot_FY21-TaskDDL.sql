--drop table APTTUS_DW.SF_PRODUCTION."Opportunity_C1_Snapshot_FY21";
--delete from APTTUS_DW.SF_PRODUCTION."Opportunity_C1_Snapshot_FY21";
--Create table APTTUS_DW.SF_PRODUCTION."Opportunity_C1_Snapshot_FY21" 
--as
;
--DROP TASK APTTUS_DW.SF_PRODUCTION.C1_OPPORTUNITY_SNAPSHOT;

CREATE OR REPLACE TASK APTTUS_DW.SNAPSHOTS.C1_OPPORTUNITY_SNAPSHOT
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 09 02 * * * America/Los_Angeles'  
AS
INSERT INTO APTTUS_DW.SF_PRODUCTION."Opportunity_C1_Snapshot_FY21"
SELECT ac.NAME                                                  AS "Account_Name" 
       , ac.ID                                                  AS "AccountID"  
       , op.id                                                  AS "OpportunityID" 
       , op.name                                                AS "Opportunity Name"       
       , Current_date()                                         AS "Snapshot Date"         
       , ao.name                                                AS "Account Owner Name"
       , B.NAME                                                 AS "Segment Territory Name"
       , C.NAME                                                 AS "Account Region" 
       , C.NAME                                                 AS "Region"         -- account region is the region for C1
       , D.NAME                                                 AS "Geo"
       , E.NAME                                                 AS "TM Division Name" 
       , N.NAME                                                 AS "TM Segment Name"
       , op.OPPORTUNITY_SEGMENT_STAMP__C                        AS "Segment"
       , H.NAME                                                 AS "Division Territory Name"   
       , op.CS_DIVISION__C                                      AS "CS Division"
       , case 
           when op.CS_DIVISION__C like '%1%' then '1'
           when op.CS_DIVISION__C like '%2%' then '2'
           when op.CS_DIVISION__C like '%3%' then '3'
           when op.CS_DIVISION__C like '%4%' then '4'
           when op.CS_DIVISION__C like '%5%' then '5'
          else null
         end                                                    AS "Division Bucket"       
       , ac.BILLINGCOUNTRY                                      AS "Account.BillingCountry" 
       , ac.BILLINGSTATE                                        AS "Account.BillingState"
       , ac.SHIPPINGCOUNTRY                                     AS "Account.ShippingCountry"            
       , ac.SHIPPINGSTATE                                       AS "Account.ShippingState"
       , ac.createddate                                         AS "Account.CreatedDate" 
       , ac.industry                                            AS "Account.Industry"      
       , op.NET_NEW_MRR__C * 12                                 AS "ARR" 
       , op.NET_NEW_AVG_ACV__C                                  AS "Average ACV"   
       , op.NET_NEW_FIRST_YEAR_ACV__C                           AS "Fist Year Billings"  
       , op.NET_NEW_FIRST_YEAR_ACV_OVERRIDE__C                  AS "Fist Year Billings Override"
       , op.NET_NEW_MRR__C                                      AS "Net New MRR"
       , op.NET_NEW_DISC_RECAP_ACV__C                           AS "Net New Discount Recapture ACV" 
       , op.DISCOUNT_RECAPTURE_AVE_MRR__C                       AS "Discount Recapture MRR" 
       , op.TCV_SUBSCRIPTIONS__C                                AS "TCV Subscriptions"  -- total contract value
       , op.TCV_NON_RECURRING__C                                AS "TCV Non Recurring"  -- total contract value
       , op.TCV_SERVICES__C                                     AS "TCV Services" -- total contract value       
       , TO_DATE(op.A1_ALIGNED_BOOKINGS_DATE__C)                AS "A1 Aligned Bookings Date"
       , TO_DATE(op.BOOKINGS_DATE__C)                           AS "Bookings Date"       
       , TO_DATE(op.CLOSEDATE)                                  AS "Estimated Close Date"  
       , To_date(op.createddate)                                AS "CreatedDate"         
       , CASE
            WHEN op.STAGENAME IN ('Closed Won', '6 - Closed Won')
             AND op.SALES_OPS_APPROVED__C = false        
               THEN 'Most Likely'
          else op.FORECASTCATEGORYNAME            
         END                                                    AS "Forecast Category" 
       , op.leadsource                                          AS "LeadSource"  
       , op.CLOSED_REASON_CATEGORY__C                           AS "Closed Reason Category" -- could be Loss_Reason__c with a case
       , op.CLOSED_REASON__C                                    AS "Closed Reason"
       , op.TYPE                                                AS "Type" 
       , CASE
           WHEN op.TYPE IN ('New Business', 'Existing Business', 'Renewal')
             THEN op.TYPE
           ELSE  coalesce(concat('C1 type not mapped - ', op.TYPE),  'C1 type not mapped - N/A')        
          END                                                   AS "Predicted C2 Type"   
       , op.Sub_Type__C                                         AS "Sub Type"                  
       , op.STAGENAME                                           AS "StageName" 
       , CASE   
            WHEN op.STAGENAME IN ('Qualify', '0 - Qualification')
                THEN '0 - Qualification'
            WHEN op.STAGENAME IN ('Business Evaluation', '1 - Discovery')
                THEN '1 - Discovery'
            WHEN op.STAGENAME IN ('Solution','2 - Validation')
                THEN '2 - Validation'
            WHEN op.STAGENAME IN ('Technical Evaluation', '3 - Justification')
                THEN '3 - Justification'
            WHEN op.STAGENAME IN ('Renewal In Process','Renewal Audit Complete','Negotiation', '4 - Negotiation','Pending Renewal','5 - Pending Closed Won')
                THEN '4 - Negotiation'
            WHEN op.STAGENAME IN ('Closed Adjusted')
                THEN '6 - Closed Won'  
            WHEN op.STAGENAME IN ('Closed Won', '6 - Closed Won')
             AND op.SALES_OPS_APPROVED__C = true
                THEN '6 - Closed Won'  
            WHEN op.STAGENAME IN ('Closed Won', '6 - Closed Won')
             AND op.SALES_OPS_APPROVED__C = false                
                 THEN '4 - Negotiation'
            WHEN  op.STAGENAME IN ('Closed Lost', 'Closed Expired', 'Terminated - Non-Payment', 'Cancelled','7 - Closed Lost')
                Then '7 - Closed Lost'            
          else concat('C1 Stage not mapped - ', op.STAGENAME)        
         END                                                    AS "Predicted C2 Stage" 
       , uo.NAME                                                AS "Opportunity Owner"  
       , utm.NAME                                               AS "Territory Manager"
       , csm.NAME                                               AS "Customer Success Manager" 
       , ubo.NAME                                               AS "Booking Owner" 
       , use.NAME                                               AS "Sales Engineer"
       , op.OPPTY_CHANNEL_SOURCE__C                             AS "Opportunity Source"  
       , op.nextstep                                            AS "Next Steps" 
       , TO_DATE(op.LASTACTIVITYDATE)                           AS "Next Step Last Edited" 
       , TO_DATE(op.MRR_SUB_END__C)                             AS "MRR Sub End" 
       , TO_DATE(op.MRR_SUB_START__C)                           AS "MRR Sub Start"        
       , op.COVID19RISK__C                                      AS "COVID19RISK"
       , op.CS_FORECAST__C*12                                   AS "CS Forecast ARR"
       , op.CS_FORECAST_OVERRIDE__C                             AS "CS Forecast Override"
       , cast(op.CS_FORECAST_OVERRIDE__C*12 as varchar)         as "CS Forecast ARR Override"
       , op.CURRENT_AVE_MRR_TOTALFX__C*12                       as "Current Total ARR"
       , op.FUTURE_AVE_MRR_TOTALFX__C*12                        as "Future Total ARR"
       , (coalesce(op.DISCOUNT_RECAPTURE_AVE_MRR__C,0))*12      as "Price Increase ARR"
       , op.TOTAL_MRR_CHURNFX__C                                AS "Total MRR Churn"
       , op.TOTAL_MRR_DOWNSELLFX__C                             AS "Total MRR Downsell" 
       , op.MRR_GNMRR__C                                        AS "MRR GNMRR" 
-- new fields added 9/28/2020
       , op.SALES_MRR__C                                        AS "Sales MRR"    
       , op.PRODUCT_OF_INTEREST__C                              AS "Product of Interest"  
FROM                   "APTTUS_DW"."SF_CONGA1_1"."OPPORTUNITY" op  
LEFT OUTER JOIN        "APTTUS_DW"."SF_CONGA1_1"."ACCOUNT" ac
                    ON op.ACCOUNTID = ac.ACCOUNTID_18__C 
LEFT OUTER JOIN        "APTTUS_DW"."SF_CONGA1_1"."USER" ao 
                    ON ac.OWNERID = ao.ID                  
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.TM_TERRITORY__C B 
                    ON ac.TM_Segment_TerritoryId__c = B.ID 
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.TM_REGION__C C 
                    ON ac.TM_REGIONID__C = C.ID
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.TM_GEO__C D 
                    ON ac.TM_GEOID__C = D.ID 
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.TM_DIVISION__C E 
                    ON ac.TM_DIVISIONID__C = E.ID 
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.TM_TERRITORY__C H 
                    ON ac.TM_Segment_TerritoryId__c = H.ID  
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.TM_SEGMENT__C N 
                    ON ac.TM_SEGMENTID__C = N.ID
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.USER uo 
                    ON op.OWNERID = uo.ID 
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.USER utm 
                    ON op.TM_TERRITORY_MANAGERID__C = utm.ID 
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.USER csm 
                    ON op.CUSTOMER_SUCCESS_MANAGERID__C = csm.ID 
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.USER ubo 
                    ON op.BOOKINGS_OWNERID__C = ubo.ID
LEFT OUTER JOIN         APTTUS_DW.SF_CONGA1_1.USER use 
                    ON op.SE_ASSIGNED__C = use.ID
WHERE  ac.TEST_ACCOUNT__C = false 
  AND NOT EQUAL_NULL (op.CLOSED_REASON__C, 'Duplicate')
  AND NOT EQUAL_NULL (op.CLOSED_REASON_CATEGORY__C, 'Duplicate')
  AND NOT EQUAL_NULL (op.BILLING_FREQUENCY__C, 'Consolidated - Annual')  
  AND  op.ISDELETED = false
;
show tasks -- here to prevent errors
-- these were considered but not added -- may still add them
--       , "Average ACV"                                          AS "ACV (USD)" -- duplicate of  "Average ACV" 
--       , "A1 Aligned Bookings Date"                             AS "Close Date" -- duplicate of "A1 Aligned Bookings Date"
--       , "CreatedDate"                                          AS "Created Date" -- duplicate of "CreatedDate"
--       , "Predicted C2 Stage"                                   AS "Stage" -- duplicate of "Predicted C2 Stage"    
--       , "OpportunityID"                                        AS "Opportunity 18 Digit ID" -- duplicate of  "OpportunityID"
--       , "Predicted C2 Type"                                    AS "C2 Type" -- duplicate OF "Predicted C2 Type"  
/* things not found                                                   
       , ac.sub_region__c                                       "Account Sub Region" 
       , ac.vertical__c                                         "Account Vertical" 
       , op.highest_achieved_stage__c                           "Highest_Achieved_Stage__c" 
       , op.loss_reason__c                                      "Loss_Reason__c" 
       , '2000-01-01'                                           "Lost Master Date" 
       , ac.mintigo_account_rank_clm__c                         "Mintigo - Predictive CLM Rank" 
       , ac.mintigo_score_clm__c                                "Mintigo - Predictive CLM Score"
       , ac.mintigo_account_rank_cpq__c                         "Mintigo - Predictive CPQ Rank" 
       , ac.mintigo_score_cpq__c                                "Mintigo - Predictive CPQ Score"
       , ac.mintigo_account_rank_qtc__c                         "Mintigo - Predictive QTC Rank" 
       , ac.mintigo_score_qtc__c                                "Mintigo - Predictive QTC Score"
       , op.platform__c                                         "Platform" 
       , op.products_of_interest__c                             "Product of Interest"
       , op.ramped_acv__c / b."FX Rate"                         AS "Ramped ACV (USD)" 
       , op.sub_region__c                                       "Sub Region" 
       , op.won_lost_date__c                                    "Won_Lost_Date__c" 
       , op.x18_digit_old_sfdc_id__c                            "X18_Digit_Old_SFDC_ID__c" 
       , op.next_step_last_edited__c                            AS "Next Step Last Edited" 
 
*/
;
describe task APTTUS_DW.SNAPSHOTS.C1_OPPORTUNITY_SNAPSHOT;
alter task APTTUS_DW.SNAPSHOTS.C1_OPPORTUNITY_SNAPSHOT resume;