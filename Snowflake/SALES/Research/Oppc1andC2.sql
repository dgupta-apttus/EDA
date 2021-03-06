-- APTTUS_DW.SF_CONGA1_0."Opportunity_C1" source

CREATE OR REPLACE VIEW APTTUS_DW.SF_CONGA1_0."Opportunity_C1"  COMMENT = 'all purpose Conga1.0 Opportunity view
 9/16 Caitlin -- added Owner Role and X15 Date fields
 9/29 Greg -- added A.PRODUCT_OF_INTEREST__C AS PRIMARY_PRODUCT_OF_INTEREST it was just being set to hull
' AS 
SELECT A.NAME                              AS OPPORTUNITY_NAME 
       , A.ID                              AS OPPORTUNITY_ID
       , A.APTTUS_OPPORTUNITY_RECORD_ID__C AS APTTUS_OPPTY_ID
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
       , A.PRODUCT_OF_INTEREST__C          AS PRIMARY_PRODUCT_OF_INTEREST
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
       , 'https://getconga.lightning.force.com/lightning/r/Opportunity/' || A.ID || '/view' AS OPPORTUNITYURL
       , 'https://getconga.lightning.force.com/lightning/r/Account/' || A.ACCOUNTID || '/view' AS ACCOUNTURL        
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
  
 
-- APTTUS_DW.SF_PRODUCTION."Opportunity_C2" source

--CREATE OR REPLACE VIEW "Opportunity_C2"  COMMENT = 'all purpose Opportunity view for Blended Conga2.0' AS 
SELECT 'Conga1.0' AS CRM_SOURCE,
    NULL AS A1_PARTNER,
    ACCOUNTID AS ACCOUNT_ID,
    ACCOUNT_NAME,
    ACCOUNT_OWNER_NAME,
    ACCOUNTURL AS ACCOUNT_URL,
    (FUTURE_AVG_MRR_TOTAL * 12) AS ANNUAL_RENEWAL,
    ARR::NUMBER(19,2) AS ARR,
    OPPORTUNITY_AGE AS AGE_DAYS,
    (OPPORTUNITY_AGE/365)*12 AS AGE_MONTHS,
    AVERAGE_ACV::NUMBER(19,2) AS AVERAGE_ACV,
    BILLING_FREQUENCY__C AS BILLING_FREQUENCY,
    BOOKINGS_DATE,
    OPPORTUNITY_BOOKING_STAMP__C AS BOOKING_STAMP,
    PARTNER__C AS C1_PARTNER,
    PREDICTED_C2_STAGE AS C2_STAGE,
    PREDICTED_C2_TYPE AS C2_TYPE,
    CAMPAIGNID AS CAMPAIGN_ID,
    A1_ALIGNED_BOOKINGS_DATE AS CLOSE_BOOKINGS_DATE,
    CLOSED_REASON_CATEGORY,
    NULL AS CLOSED_REASON_DETAILS,
    CLOSED_REASON_NOTES,
    NULL AS CLOSED_REASON_SUBCATEGORY_LOSS,
    CREATEDDATE AS CREATED_DATE,
    'USD' AS CURRENCY,
    1.0 AS CURRENCY_CONVERSION_RATE,
    CURRENT_AVG_MRR_TOTAL,
    CUSTOMER_SUCCESS_MANAGERID__C AS CUSTOMER_SUPPORT_ID,
    CUSTOMER_SUCCESS_MANAGER_NAME AS CUSTOMER_SUPPORT_NAME,
    DISCOUNT_RECAPTURE_MRR::NUMBER(19,2) AS DISCOUNT_RECAPTURE_MRR,
    NULL AS DOWNSELL_CHURN_CATEGORY, 
    NULL AS DOWNSELL_CHURN_SUB_CATEGORY,
    MRR_SUB_END AS END_DATE,
    ESTIMATED_CLOSE_DATE,
    NULL AS EXPANSION_DOLLARS,
    FIRST_YEAR_BILLINGS::NUMBER(19,2) AS FIRST_YEARS_BILLINGS,
    FISCAL_WEEK,
    FORECAST_CATEGORY,
    CASE
        WHEN GEO_NAME = 'NA'
           THEN 'AMER'
        ELSE GEO_NAME
    END AS GEO,
    HIGHEST_ACHIEVED_STAGE,
    NULL AS INBOUND_OUTBOUND_OPPORTUNITY,
    LAST_EDIT_DATE,
    LEAD_SOURCE,
    LEAD_SOURCE_DETAIL,
    MRR_SUB_END__C AS MRR_SUB_END,
    MRR_SUB_START__C  AS MRR_SUB_START,
    NET_NEW_MRR::NUMBER(19,2) AS NET_NEW_MRR,
    NEXTSTEP AS NEXT_STEP,
    NULL AS NEXT_STEP_LAST_EDITED,
    NN_DISCOUNT_RECAPTURE_ACV::NUMBER(19,2) AS NN_DISCOUNT_RECAPTURE_ACV,
    OPPTY_CHANNEL_SOURCE AS OPPORTUNITY_CHANNEL_SOURCE,
    OPPORTUNITY_ID,
    OPPORTUNITY_NAME,
    OPPORTUNITYURL AS OPPORTUNITY_URL,
    OPS_APPROVED,
    OPPORTUNITY_OWNER_GEO_STAMP__C AS OWNER_GEO_STAMP,
    OWNERID AS OWNER_ID,
    OWNER_NAME,
    OWNER_ROLE,
    NULL AS PLATFORM,
    COMPETITOR AS PRIMARY_COMPETITOR,
    NULL AS PRIMARY_QUOTE_ID,
    PRIMARY_PRODUCT_OF_INTEREST AS PRODUCTS_OF_INTEREST,
    PROBABILITY,
    NULL AS RAMPED_ACV,
    REGION_NAME AS REGION,
    NULL AS RENEWED_AMOUNT,
    CASE
        WHEN TYPE = 'Renewal'
            THEN ARR
        ELSE 0 END RENEWAL_DOLLARS,
    (CURRENT_AVG_MRR_TOTAL * 12) AS RENEWAL_DUE,
    DATEADD(DD,-1,MRR_SUB_START) AS RENEWAL_DUE_DATE,
    ((CURRENT_AVG_MRR_TOTAL + CS_FORECAST_MRR) * 12) AS RENEWAL_FORECAST_ARR,
    NULL AS RENEWAL_UPLIFT,
    SALES_ENGINEER_NAME,
    SALES_MRR,
    SALES_ACCEPTED AS SALES_OPPORTUNITY_ACCEPTED,
    NULL AS SALES_OPPORTUNITY_ACCEPTED_DATE,
    SALESFORCE_ORG__C AS SALESFORCE_ORG,
    SE_ASSIGNED__C AS SE_ID,
    SEGMENT,
    STAGENAME AS STAGE,
    NULL AS STAGE_1_DATE_OF_ENTRY,
    MRR_SUB_START AS START_DATE,
    SUBTYPE AS SUB_TYPE,
    TCV_NON_RECURRING::NUMBER(19,2) AS TCV_NON_RECURRING,
    TCV_SERVCES::NUMBER(19,2) AS TCV_SERVICES,
    TCV_SUBSCRIPTIONS::NUMBER(19,2) AS TCV_SUBSCRIPTIONS,
    DATEDIFF(month, MRR_SUB_END, MRR_SUB_START) AS TERM_MONTHS,
    SEGMENT_TERRITORY_NAME AS TERRITORY,
    TM_TERRITORY_MANAGERID__C as TERRITORY_MANAGER_ID,
    TERRITORY_MANAGER_NAME,
    TM_SEGMENT_NAME,
    NULL AS TOTAL_DEAL_VALUE,
    NULL AS TOTAL_RENEWAL_DUE,
    NULL AS TOTAL_UPSELL_DOWNSELL,
    TYPE,
    NULL AS VALID_UNTIL_DATE,
    X15_DATE__C AS X15_DATE,
    APTTUS_OPPTY_ID AS XOPPORTUNITY_ID
FROM APTTUS_DW.SF_CONGA1_0."Opportunity_C1"     
UNION
SELECT 'Apttus1.0' AS CRM_SOURCE,
    PARTNER_SOURCE__C AS A1_PARTNER,
    ACCOUNTID AS ACCOUNT_ID,
    ACCOUNT_NAME,
    ACCOUNT_OWNER_NAME,
    ACCOUNTURL AS ACCOUNT_URL,
    ANNUAL_RENEWAL,
    FIRST_YEARS_BILLINGS::NUMBER(19,2) AS ARR,
    AGE_OF_THE_DEAL_DAYS AS AGE_DAYS,
    AGE_OF_THE_DEAL_MONTHS AS AGE_MONTHS,
    NULL AS AVERAGE_ACV,
    NULL AS BILLING_FREQUENCY,
    CLOSEDATE AS BOOKINGS_DATE,
    'Apttus' AS BOOKING_STAMP,
    NULL AS C1_PARTNER,
    PREDICTED_C2_STAGE AS C2_STAGE,
    PREDICTED_C2_TYPE AS C2_TYPE,
    CAMPAIGNID AS CAMPAIGN_ID,
    CLOSEDATE AS CLOSE_BOOKINGS_DATE,
    COALESCE(CLOSED_REASON_CATEGORY_LOSS, CLOSED_REASON_CATEGORY_WIN) AS CLOSED_REASON_CATEGORY,
    NULL AS CLOSED_REASON_DETAILS,
    NULL AS CLOSED_REASON_NOTES,
    CLOSED_REASON_SUBCATEGORY_LOSS,
    CREATEDDATE AS CREATED_DATE,
    CURRENCY,
    CURRENCY_CONVERSION_RATE,
    NULL AS CURRENT_AVG_MRR_TOTAL,
    NULL AS CUSTOMER_SUPPORT_ID,
    ACCOUNT_CUSTOMER_MANAGER_NAME AS CUSTOMER_SUPPORT_NAME,
    NULL AS DISCOUNT_RECAPTURE_MRR,
    DOWNSELL_CHURN_CATEGORY, 
    DOWNSELL_CHURN_SUB_CATEGORY,
    VALID_UNTIL_DATE__C AS END_DATE,
    CLOSEDATE AS ESTIMATED_CLOSE_DATE,
    EXPANSION_DOLLARS,
    FIRST_YEARS_BILLINGS::NUMBER(19,2) as FIRST_YEARS_BILLINGS,
    FISCAL_WEEK,
    FORECAST_CATEGORY,
    GEO,
    HIGHEST_ACHIEVED_STAGE,
    INBOUND_OUTBOUND_OPPORTUNITY__C AS INBOUND_OUTBOUND_OPPORTUNITY,
    LAST_EDIT_DATE,
    LEAD_SOURCE,
    NULL AS LEAD_SOURCE_DETAIL,
    NULL AS MRR_SUB_END,
    NULL AS MRR_SUB_START,
    NULL AS NET_NEW_MRR,
    NEXTSTEP AS NEXT_STEP,
    NEXT_STEP_LAST_EDITED__C AS NEXT_STEP_LAST_EDITED,
    NULL AS NN_DISCOUNT_RECAPTURE_ACV,
    OPPORTUNITY_SOURCE AS OPPORTUNITY_CHANNEL_SOURCE,
    OPPORTUNITY_ID,
    OPPORTUNITY_NAME,
    OPPORTUNITYURL AS OPPORTUNITY_URL,
    OPS_APPROVED,
    GEO AS OWNER_GEO_STAMP,
    OWNERID AS OWNER_ID,
    OWNER_NAME,
    OWNER_ROLE,
    PLATFORM,
    PRIMARY_COMPETITOR,
    PRIMARY_QUOTE_ID,
    PRODUCTS_OF_INTEREST__C AS PRODUCTS_OF_INTEREST,
    PROBABILITY,
    RAMPED_ACV__C::NUMBER(19,2) AS RAMPED_ACV,
    REGION AS REGION,
    RENEWED_AMOUNT,
    RENEWAL_DOLLARS,
    RENEWAL_DUE,
    RENEWAL_DUE_DATE,
    (RENEWAL_DOLLARS + RENEWAL_UPLIFT) AS RENEWAL_FORECAST_ARR,
    RENEWAL_UPLIFT,
    SALES_ENGINEER AS SALES_ENGINEER_NAME,
    NULL AS SALES_MRR,
    OPPORTUNITY_ACCEPTED__C AS SALES_OPPORTUNITY_ACCEPTED,
    OPPORTUNITY_ACCEPTED_DATE__C AS SALES_OPPORTUNITY_ACCEPTED_DATE,
    NULL AS SALESFORCE_ORG,
    APTTUS_SE__C AS SE_ID,
    SEGMENT__C AS SEGMENT,
    STAGENAME AS STAGE,
    STAGE_1_DATE_OF_ENTRY__C AS STAGE_1_DATE_OF_ENTRY,
    SUBSCRIPTION_START_DATE AS START_DATE,
    SUBTYPE AS SUB_TYPE,
    NULL AS TCV_NON_RECURRING,
    NULL AS TCV_SERVICES,
    NULL AS TCV_SUBSCRIPTIONS,
    TERM_MONTHS,
    TERRITORY_NAME AS TERRITORY,
    OWNERID AS TERRITORY_MANAGER_ID,
    OWNER_NAME AS TERRITORY_MANAGER_NAME,
    NULL AS TM_SEGMENT_NAME,
    TOTAL_DEAL_VALUE::NUMBER(19,2) AS TOTAL_DEAL_VALUE,
    TOTAL_RENEWAL_DUE__C::NUMBER(19,2) AS TOTAL_RENEWAL_DUE,
    TOTAL_UPSELL_DOWNSELL,
    TYPE,
    VALID_UNTIL_DATE__C AS VALID_UNTIL_DATE,
    NULL AS X15_DATE,
    CONGA1_OPPTY_ID AS XOPPORTUNITY_ID
FROM APTTUS_DW.SF_PRODUCTION."Opportunity_A1"
WHERE TYPE <> 'Services'
  AND IGNORE_TESTOPPORTUNITIES = 1; 