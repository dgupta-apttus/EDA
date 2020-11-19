-- APTTUS_DW.SF_CONGA1_0."Opportunity_C1" source

CREATE OR REPLACE VIEW APTTUS_DW.SF_CONGA1_0."Opportunity_C1"  
COMMENT = 'all purpose Conga1.0 Opportunity view
 9/16 Caitlin -- added Owner Role and X15 Date fields
 10/09 Greg added the snapshot fields AGAIN' AS 
SELECT A.NAME                              AS OPPORTUNITY_NAME 
       , A.ID                              AS OPPORTUNITY_ID
       , A.APTTUS_OPPORTUNITY_ID__C AS APTTUS_OPPTY_ID
       , TO_DATE(A.A1_ALIGNED_BOOKINGS_DATE__C)     AS A1_ALIGNED_BOOKINGS_DATE
       , TO_DATE(A.BOOKINGS_DATE__C)                AS BOOKINGS_DATE       
       , TO_DATE(A.CLOSEDATE)                       AS ESTIMATED_CLOSE_DATE 
       , A.TYPE                            AS TYPE
       , CASE
           WHEN A.TYPE IN ('New Business', 'Existing Business', 'Renewal')
             THEN A.TYPE
           ELSE  coalesce(concat('C1 type not mapped - ', A.TYPE),  'C1 type not mapped - N/A')        
          END                              AS PREDICTED_C2_TYPE        
       , A.STAGENAME                       AS STAGENAME
       , CASE   
            WHEN A.STAGENAME IN ('Qualify', '0 - Qualification')
                THEN '0 - Qualification'
            WHEN A.STAGENAME IN ('Business Evaluation', '1 - Discovery')
                THEN '1 - Discovery'
            WHEN A.STAGENAME IN ('Solution','2 - Validation')
                THEN '2 - Validation'
            WHEN A.STAGENAME IN ('Technical Evaluation', '3 - Justification')
                THEN '3 - Justification'
            WHEN A.STAGENAME IN ('Renewal In Process','Renewal Audit Complete','Negotiation', '4 - Negotiation','Pending Renewal','5 - Pending Closed Won')
                THEN '4 - Negotiation'
            WHEN A.STAGENAME IN ('Closed Adjusted')
                THEN '6 - Closed Won'  
            WHEN A.STAGENAME IN ('Closed Won', '6 - Closed Won')
             AND A.SALES_OPS_APPROVED__C = true
                THEN '6 - Closed Won'  
            WHEN A.STAGENAME IN ('Closed Won', '6 - Closed Won')
             AND A.SALES_OPS_APPROVED__C = false                
                 THEN '4 - Negotiation'
            WHEN  A.STAGENAME IN ('Closed Lost', 'Closed Expired', 'Terminated - Non-Payment', 'Cancelled','7 - Closed Lost')
                Then '7 - Closed Lost'            
          else concat('C1 Stage not mapped - ', A.STAGENAME)        
         END                               AS PREDICTED_C2_STAGE
       , A.ACCOUNTID 
       , B.ACCOUNT_NAME                    AS ACCOUNT_NAME 
       , A.OWNERID 
       , C.NAME                            AS OWNER_NAME 
       , C.TITLE                           AS OWNER_ROLE
       , B.OWNER_NAME                      AS ACCOUNT_OWNER_NAME
       , A.BOOKINGS_OWNERID__C
       , F.NAME                            AS BOOKINGS_OWNER_NAME        
       , A.TM_TERRITORY_MANAGERID__C 
       , D.NAME                            AS TERRITORY_MANAGER_NAME 
       , B.TERRITORY_MANAGER_NAME          AS ACCOUNT_TERRITORY_MANAGER_NAME          
       , A.CUSTOMER_SUCCESS_MANAGERID__C 
       --, A.Customer_Success_Manager__c -- deprecated do no use                
       , E.NAME                            AS CUSTOMER_SUCCESS_MANAGER_NAME
       , B.CUSTOMER_SUCCESS_MANAGER_NAME   AS ACCOUNT_CUSTOMER_SUCCESS_MANAGER_NAME
        --, A.Customer_Success_Manager__c -- deprecated!
       , A.SE_ASSIGNED__C 
       , G.NAME                            AS SALES_ENGINEER_NAME       
       , A.SUB_TYPE__C                     AS SUBTYPE
       , A.OPPTY_CHANNEL_SOURCE_2__C       AS OPPTY_CHANNEL_SOURCE -- match to OPPORTUNITY_SOURCE
       , A.SALES_OPS_APPROVED__C           AS OPS_APPROVED 
       , A.OPPORTUNITY_AGE__C              AS OPPORTUNITY_AGE  
       , CASE
            WHEN A.STAGENAME IN ('Closed Won', '6 - Closed Won')
             AND A.SALES_OPS_APPROVED__C = false        
               THEN 'Most Likely'
          else A.FORECASTCATEGORYNAME            
         END                               AS FORECAST_CATEGORY          
       , A.LEADSOURCE                      AS LEAD_SOURCE
       , A.LEAD_SOURCE_DETAIL__C           AS LEAD_SOURCE_DETAIL
-- geo fields all come from account
       , B.GEO_NAME__N                     AS GEO_NAME
       , A.OPPORTUNITY_OWNER_GEO_STAMP__C 
       , B.REGION_NAME
       --, A.REGION_2__C -- this is older REgion or GEO 
       , B.TM_SEGMENT_NAME
       , A.OPPORTUNITY_SEGMENT_STAMP__C    AS SEGMENT       
       , B.SEGMENT_TERRITORY_NAME
       , A.OPPORTUNITY_BOOKING_STAMP__C
       , B.TM_DIVISION_NAME
       , B.DIVISION_TERRITORY_NAME
-- geo fields up 
       , A.SALESFORCE_ORG__C    
       , A.SALES_MRR__C * 12                    AS ARR 
       , A.NET_NEW_AVG_ACV__C                   AS AVERAGE_ACV   
       , A.NET_NEW_FIRST_YEAR_ACV__C            AS FIRST_YEAR_BILLINGS  
       , A.NET_NEW_FIRST_YEAR_ACV_OVERRIDE__C   AS FIRST_YEAR_BILLINGS_OVERRIDE
       , A.NET_NEW_MRR__C                       AS NET_NEW_MRR
       , A.SALES_MRR__C                         AS SALES_MRR
       , A.FUTURE_AVE_MRR_TOTALFX__C            AS FUTURE_AVG_MRR_TOTAL
       , A.CS_FORECAST__C                        AS CS_FORECAST_MRR
       , A.NET_NEW_DISC_RECAP_ACV__C            AS NN_DISCOUNT_RECAPTURE_ACV
       , A.CURRENT_AVE_MRR_TOTALFX__C           AS CURRENT_AVG_MRR_TOTAL
       , A.DISCOUNT_RECAPTURE_AVE_MRR__C        AS DISCOUNT_RECAPTURE_MRR
       , TO_DATE(A.MRR_SUB_END__C)              AS MRR_SUB_END
       , TO_DATE(A.MRR_SUB_START__C)            AS MRR_SUB_START
       , TCV_SUBSCRIPTIONS__C                   AS TCV_SUBSCRIPTIONS -- total contract value
       , TCV_NON_RECURRING__C                   AS TCV_NON_RECURRING -- total contract value
       , TCV_SERVICES__C                        AS TCV_SERVCES -- total contract value             
       , NULL                              AS SALES_ACCEPTED 
       , TO_DATE(A.MRR_SUB_END__C)         AS MRR_SUB_END__C 
       , TO_DATE(A.MRR_SUB_START__C)       AS MRR_SUB_START__C  
       , TO_DATE(A.CREATEDDATE)            AS CREATEDDATE 
       , NULL                              AS PRIMARY_PRODUCT_OF_INTEREST
       , A.CLOSED_WON_LOST_COMPETITOR__C   AS COMPETITOR      
       , A.CLOSED_REASON_CATEGORY__C       AS CLOSED_REASON_CATEGORY  
       , A.CLOSED_REASON_NOTES__C          AS CLOSED_REASON_NOTES
       , A.NEXTSTEP 
       , TO_DATE(A.LASTACTIVITYDATE)       AS LAST_EDIT_DATE 
       , NULL                              AS FISCAL_WEEK 
       , NULL                              AS HIGHEST_ACHIEVED_STAGE 
       , B.TEST_ACCOUNT__C
       , A.BILLING_FREQUENCY__C
       , CAMPAIGNID
       , PROBABILITY
       , PARTNER__C
       , X15_DATE__C
       , PRIMARY_CONTACT__C
       , 'https://getconga.lightning.force.com/lightning/r/Opportunity/' || A.ID || '/view' AS OPPORTUNITYURL
       , 'https://getconga.lightning.force.com/lightning/r/Account/' || A.ACCOUNTID || '/view' AS ACCOUNTURL     
       , A.SYSTEMMODSTAMP AS OPPTY_MODSTAMP -- needed for last activity or last edit in snapshots
       , B.SYSTEMMODSTAMP AS ACCNT_MODSTAMP
FROM   APTTUS_DW.SF_CONGA1_0.OPPORTUNITY A 
INNER JOIN APTTUS_DW.SF_CONGA1_0."Account_C1" B 
       ON A.ACCOUNTID = B.ACCOUNTID_18__C 
LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.USER C 
            ON A.OWNERID = C.ID 
LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.USER D 
            ON A.TM_TERRITORY_MANAGERID__C = D.ID 
LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.USER E 
            ON A.CUSTOMER_SUCCESS_MANAGERID__C = E.ID 
LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.USER F 
            ON A.BOOKINGS_OWNERID__C = F.ID
LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.USER G 
            ON A.SE_ASSIGNED__C = G.ID
WHERE  B.TEST_ACCOUNT__C = false 
  AND NOT EQUAL_NULL (A.CLOSED_REASON__C, 'Duplicate')
  AND NOT EQUAL_NULL (A.CLOSED_REASON_CATEGORY__C, 'Duplicate')
  AND NOT EQUAL_NULL (A.BILLING_FREQUENCY__C, 'Consolidated - Annual')  
  AND  A.ISDELETED = False;