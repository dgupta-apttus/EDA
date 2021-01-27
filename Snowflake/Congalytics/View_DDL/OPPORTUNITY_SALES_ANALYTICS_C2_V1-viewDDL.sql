-- APTTUS_DW.SF_PRODUCTION.OPPORTUNITY_SALES_ANALYTICS_C2_V1 source

CREATE OR REPLACE VIEW APTTUS_DW.SF_PRODUCTION.OPPORTUNITY_SALES_ANALYTICS_C2_V1
COMMENT = 'Opportunity object for C2 for Sales Analytics
-- 8/26 Caitlin -- view created
-- 9/1 Caitlin -- changed date used from Close Bookings Date to Bookings Date
-- 9/2 Caitlin -- removed current fiscal year constraint, removed dollars logic
-- 9/3 Caitlin -- added Next Step Last Edited and Campaign ID
-- 9/23 Caitlin -- marketing/lead source changed to opportunity channel source, added stage 1 date of entry
-- 9/29 Caitlin -- added Booking Stamp or Sales MRR > 0 logic
-- 9/30 Caitlin -- added Ops Approved = True filter for C1
-- 10/5 Caitlin -- added Opportunity Channel Source Category to match Demand Gen Goals
-- 10/15 Caitlin -- added Contact Name and Email
-- 10/19 Ryan -- added dollar buckets from First Year Billings
-- 10/20 Caitlin -- added C1 Account Rank, and A1 CLM, CPQ, and QTC Rank for Mintigo reporting
-- 10/26 Caitlin -- added Renewal Due Date
-- 10/27 Caitlin -- added Sales Play
-- 11/4 Caitlin -- added Highest Product Family logic
-- 11/6 Greg -- added C2_PARTNER
-- 11/16 Ryan - added "Influenced Partner"
-- 11/18 Caitlin -- added SAO Date (X15 Date for C1, Stage 1 of Entry for A1) and Product List from Product Asset Rollup View
-- 11/18 Caitlin -- added Demand Gen needed fields (Billing Frequency, Closed Reason Details)
-- 11/20 Caitlin -- added OG Product Family
-- 12/8 Caitlin -- added Sub Type <> Quoting Opportunity and Owner Role <> Program Associate
-- 12/14 Caitlin -- added SAO Time to Convert Days and Months, Opportunity Channel Source Order By, and Deal Cycle Days and Months
-- 1/8 Caitlin -- added previous week snapshot fields
-- 1/12 Caitlin -- coalesced Geo and Owner Geo Stamp, coalesced segment with Commercial, and coalesced channel source with Sales as per Marketing
-- 1/21 Caitlin -- added Loss Reason <> Duplicate
-- 1/21 Ryan -- Added Boolean "Has" to Partner Sourced and Influenced Partner
-- 1/26 Ryan -- Added SEGMENT_HIERARCHY & SEGMENT_ROLLUP_ORDER_BY
-- 1/27 Ryan -- Added C2_STAGE_ORDERBY
'
AS
WITH
HIGHEST_PRODUCT_FAMILY AS(
    SELECT * FROM(
        SELECT
            A.ALLOCATED_ARR,
            A.OPPORTUNITY_ID,
            A.OG_PRODUCT_FAMILY,
            A.PRODUCT_LINE,
            A.PRODUCT_FAMILY,
            ROW_NUMBER() OVER (PARTITION BY OPPORTUNITY_ID ORDER BY (ALLOCATED_ARR) DESC) AS ROW_NUMBER,
            SUM(ALLOCATED_ARR) OVER (PARTITION BY OPPORTUNITY_ID) AS ALLOCATED_ARR_SUM
        FROM APTTUS_DW.SF_PRODUCTION.C2_PRODUCT_ASSET_ROLLUP A
    )
    WHERE ROW_NUMBER = 1
), 

C2_PRODUCT_LISTS AS(
    SELECT 
        A.OPPORTUNITY_ID,
        COALESCE(LISTAGG(DISTINCT A.PRODUCT_FAMILY, ', ') WITHIN GROUP (ORDER BY A.PRODUCT_FAMILY), 'No Products Found') AS PRODUCT_LIST
    FROM APTTUS_DW.SF_PRODUCTION.C2_PRODUCT_ASSET_ROLLUP A
    WHERE A.ALLOCATED_ARR <> 0
    
    GROUP BY A.OPPORTUNITY_ID
),

MAX_LAST_WEEK AS(
    SELECT 
        MAX(SNAPSHOT_DATE) AS MAX_SNAP,
        CRM_SOURCE,
        OPPORTUNITY_ID
    FROM APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY
  
    WHERE SNAPSHOT_DATE <= (CURRENT_DATE() - 7)
        AND (CLOSE_BOOKINGS_DATE > (CURRENT_DATE() - 14)
                OR CLOSE_BOOKINGS_DATE IS NULL)
    
    GROUP BY CRM_SOURCE, OPPORTUNITY_ID
),

LAST_WEEK_FIELDS AS(
  SELECT 
    B.STAGE,
    B.FORECAST_CATEGORY,
    C."Fiscal Period" AS CLOSE_BOOKINGS_FISCAL_PERIOD,
    B.ARR,
    A.OPPORTUNITY_ID,
    A.CRM_SOURCE
  FROM MAX_LAST_WEEK A
  INNER JOIN APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY B
    ON A.CRM_SOURCE = B.CRM_SOURCE
        AND A.OPPORTUNITY_ID = B.OPPORTUNITY_ID
        AND A.MAX_SNAP = B.SNAPSHOT_DATE
  LEFT JOIN APTTUS_DW.SF_PRODUCTION."Dates" C
    ON B.CLOSE_BOOKINGS_DATE = C."Date"
)

SELECT
  O.A1_CLM_RANK,
  O.A1_CPQ_RANK,
  O.A1_PARTNER,
  O.A1_QTC_RANK,
  O.C1_ACCOUNT_RANK,
  O.ACCOUNT_ID,
  O.ACCOUNT_NAME,
  O.ACCOUNT_URL,
  O.AGE_DAYS,
  O.AGE_MONTHS,
  O.ARR AS C_ARR,
  O.AVERAGE_ACV,
  O.BILLING_FREQUENCY,
  O.BOOKING_STAMP,
  O.BOOKINGS_DATE,
  O.C1_PARTNER,
  O.C2_PARTNER,
  CASE 
      WHEN O.C2_PARTNER IS NULL 
           THEN 0::BOOLEAN
      ELSE 1::BOOLEAN
    END "Has Parnter Sourced",
  O.C2_STAGE,
  O.C2_TYPE,
   CASE 
    WHEN O.C2_TYPE = 'New Business'
        THEN 1
    WHEN O.C2_TYPE = 'Existing Business'
        THEN 2
    WHEN O.C2_TYPE = 'Renewal'
        THEN 3
    ELSE 10 END C2_STAGE_ORDERBY,
  O.CAMPAIGN_ID,
  O.OPPORTUNITY_CHANNEL_SOURCE,
  CASE 
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'Marketing')
          THEN 'Marketing'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'BDR')
          THEN 'Marketing'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'Digital')
          THEN 'Marketing'
      WHEN OPPORTUNITY_CHANNEL_SOURCE = 'Prospecting' AND (LEAD_SOURCE <> 'Territory Manager' OR LEAD_SOURCE <> 'Customer Success Manager')
          THEN 'SDR'
      WHEN OPPORTUNITY_CHANNEL_SOURCE = 'Prospecting' AND (LEAD_SOURCE = 'Territory Manager')
          THEN 'Sales'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'Salesforce')
          THEN 'Alliances'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'AWS')
          THEN 'Alliances'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'GTM')
          THEN 'Alliances'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'AE')
          THEN 'Sales'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'Forecasting')
          THEN 'Sales'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'Non-Partner')
          THEN 'Sales'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'N3')
          THEN 'SDR'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'Inside Sales')
          THEN 'SDR'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'SDR')
          THEN 'SDR'
      WHEN CONTAINS(OPPORTUNITY_CHANNEL_SOURCE, 'CSM')
          THEN 'CS'
      WHEN OPPORTUNITY_CHANNEL_SOURCE IN ('Partner', 'SI Partner', 'Partner Referral')
          THEN 'SI'
      ELSE 'Other' END OPPORTUNITY_CHANNEL_SOURCE_CATEGORY,
 /* O.OPPORTUNITY_CHANNEL_SOURCE AS CHANNEL_SOURCE_DETAIL,
  CASE 
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'Marketing')
          THEN 'Marketing'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'BDR')
          THEN 'Marketing'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'Digital')
          THEN 'Marketing'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'AE')
          THEN 'Sales'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'Forecasting')
          THEN 'Sales'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'Non-Partner')
          THEN 'Sales'
      WHEN CHANNEL_SOURCE_DETAIL = 'Prospecting' AND (LEAD_SOURCE = 'Territory Manager')
          THEN 'Sales'
      WHEN CHANNEL_SOURCE_DETAIL = 'Prospecting' AND LEAD_SOURCE NOT IN ('Territory Manager', 'Customer Success Manager')
          THEN 'SDR'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'Salesforce')
          THEN 'Alliances'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'AWS')
          THEN 'Alliances'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'GTM')
          THEN 'Alliances'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'N3')
          THEN 'SDR'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'Inside Sales')
          THEN 'SDR'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'SDR')
          THEN 'SDR'
      WHEN CONTAINS(CHANNEL_SOURCE_DETAIL, 'CSM')
          THEN 'CS'
      WHEN CHANNEL_SOURCE_DETAIL IN ('Partner', 'SI Partner', 'Partner Referral')
          THEN 'SI'
      ELSE 'Sales' END CHANNEL_SOURCE,
  CASE
    WHEN CHANNEL_SOURCE = 'Marketing'
        THEN 'Marketing Owned'
    WHEN CHANNEL_SOURCE = 'SDR'
        THEN 'Marketing Owned'
    WHEN CHANNEL_SOURCE = 'Sales'
        THEN 'Sales Owned'
    WHEN CHANNEL_SOURCE = 'SI'
        THEN 'Sales Owned' 
    WHEN CHANNEL_SOURCE = 'Alliances'
        THEN 'Sales Owned'  
    WHEN CHANNEL_SOURCE = 'CS'
        THEN 'Sales Owned'
    WHEN CHANNEL_SOURCE = 'Other'
        THEN 'Marketing Owned'
    END CHANNEL_SOURCE_CATEGORY,
  CASE
    WHEN CHANNEL_SOURCE = 'Marketing'
        THEN 1
    WHEN CHANNEL_SOURCE = 'SDR'
        THEN 2  
    WHEN CHANNEL_SOURCE = 'Sales'
        THEN 3 
    WHEN CHANNEL_SOURCE = 'SI'
        THEN 4 
    WHEN CHANNEL_SOURCE = 'Alliances'
        THEN 5  
    WHEN CHANNEL_SOURCE = 'CS'
        THEN 6
    WHEN CHANNEL_SOURCE = 'Other'
        THEN 7
    END CHANNEL_SOURCE_ORDER_BY,
 */ O.CLOSE_BOOKINGS_DATE,
  O.CLOSED_REASON_CATEGORY,
  O.CLOSED_REASON_DETAILS,
  O.CONTACT_ID,
  O.CREATED_DATE,
  O.CURRENCY,
  O.CURRENT_AVG_MRR_TOTAL,
  CASE 
    WHEN O.FIRST_YEARS_BILLINGS <= 50000
        THEN '< 50,000'
    WHEN O.FIRST_YEARS_BILLINGS <= 100000   
        THEN '50,000 - 100,000'
    WHEN O.FIRST_YEARS_BILLINGS <= 150000
        THEN '100,000 - 150,000'
    WHEN O.FIRST_YEARS_BILLINGS <= 200000   
        THEN '150,000 - 200,000'
    WHEN O.FIRST_YEARS_BILLINGS <= 250000
        THEN '200,000 - 250,000'
    WHEN O.FIRST_YEARS_BILLINGS <= 300000   
        THEN '250,000 - 300,000'    
    WHEN O.FIRST_YEARS_BILLINGS <= 400000   
        THEN '300,000 - 400,000'
    WHEN O.FIRST_YEARS_BILLINGS <= 500000
        THEN '400,000 - 500,000'
    WHEN O.FIRST_YEARS_BILLINGS > 500000   
        THEN '500,000+'  
  ELSE 'Unknown'
  END AS DEAL_THRESHOLD,
  O.DISCOUNT_RECAPTURE_MRR,
  O.END_DATE,
  O.ESTIMATED_CLOSE_DATE,
  O.FIRST_YEARS_BILLINGS,
  O.FORECAST_CATEGORY,
  O.FUTURE_AVG_MRR_TOTAL,
  COALESCE(O.GEO, O.OWNER_GEO_STAMP) AS GEO,
  CASE
    WHEN O.GEO = 'AMER' 
        THEN 1
    WHEN O.GEO = 'EMEA'
        THEN 2
    WHEN O.GEO = 'APAC'
        THEN 3
    WHEN O.GEO = 'Other'
        THEN 4  
    WHEN O.GEO IS NULL
        THEN 5
    ELSE 10 END GEO_ORDER_BY,
  O.HIGHEST_ACHIEVED_STAGE,
  O.LEAD_SOURCE,
  O.LEAD_SOURCE_DETAIL,
  O.NET_NEW_MRR,
  O.NEXT_STEP,
  O.NEXT_STEP_LAST_EDITED,
  O.OPPORTUNITY_ID,
  O.OPPORTUNITY_NAME,
  O.OPPORTUNITY_URL,
  CASE
    WHEN O.CRM_SOURCE = 'Conga1.0'
        THEN O.OPS_APPROVED
    ELSE TRUE END OPS_APPROVED,
  O.OWNER_GEO_STAMP,
  O.OWNER_ID,
  O.OWNER_NAME,
  O.OWNER_ROLE,		
  CASE
    WHEN O.TYPE = 'Renewal' AND O.TERRITORY_MANAGER_NAME IS NOT NULL 
        THEN O.TERRITORY_MANAGER_NAME
    ELSE O.OWNER_NAME END OWNER_C2,
  O.PRIMARY_COMPETITOR,
  O.PRIMARY_QUOTE_ID,
  O.PROBABILITY,
  O.PRODUCTS_OF_INTEREST,
  O.REGION,
  O.RENEWAL_DOLLARS,
  O.RENEWAL_DUE_DATE,
  O.SALES_MRR,
  O.SALES_OPPORTUNITY_ACCEPTED,
  O.SALES_OPPORTUNITY_ACCEPTED_DATE,
  O.SALES_PLAY,
  CASE
    WHEN O.CRM_SOURCE = 'Conga1.0'
        THEN O.X15_DATE
    WHEN O.CRM_SOURCE = 'Apttus1.0'
        THEN O.STAGE_1_DATE_OF_ENTRY
  END SAO_DATE,
  DATEDIFF(day, O.CREATED_DATE, SAO_DATE) AS SAO_TIME_DAYS,
  DATEDIFF(month, O.CREATED_DATE, SAO_DATE) AS SAO_TIME_MONTHS,
  DATEDIFF(day, SAO_DATE, O.CLOSE_BOOKINGS_DATE) AS DEAL_CYCLE_DAYS,
  DATEDIFF(month, SAO_DATE, O.CLOSE_BOOKINGS_DATE) AS DEAL_CYCLE_MONTHS,
  CASE
    WHEN O.SEGMENT IN ('Growth', 'Growth Commercial')
        THEN 'Growth Commercial'
    WHEN O.SEGMENT IN ('Other')
        THEN 'Commercial'
    ELSE COALESCE(O.SEGMENT, 'Commercial') END SEGMENT,
  CASE 
    WHEN O.SEGMENT = 'Strategic'
        THEN 1
    WHEN O.SEGMENT = 'Enterprise'
        THEN 2
    WHEN O.SEGMENT = 'General Commercial'
        THEN 3
    WHEN O.SEGMENT = 'Mid Commercial'
        THEN 4
    WHEN O.SEGMENT = 'Growth Commercial'
        THEN 5
    WHEN O.SEGMENT = 'Growth'
        THEN 6
    WHEN O.SEGMENT = 'Partner'
        THEN 7
    WHEN O.SEGMENT = 'Customer Success 1'
        THEN 8
    WHEN O.SEGMENT = 'Customer Success 2'
        THEN 9
    WHEN O.SEGMENT = 'Customer Success 3'
        THEN 10
    WHEN O.SEGMENT = 'Customer Success 4'
        THEN 11
    WHEN O.SEGMENT = 'Other'
        THEN 12
    ELSE 20 END AS SEGMENT_ORDER_BY,
  CASE
    WHEN O.SEGMENT IN ('Growth', 'Growth Commercial','General Commercial','Mid Commercial','Other')
        THEN 'Commercial'
    ELSE COALESCE(O.SEGMENT, 'Commercial') END SEGMENT_HIERARCHY,
  CASE 
    WHEN SEGMENT_HIERARCHY = 'Strategic'
        THEN 1
    WHEN SEGMENT_HIERARCHY = 'Enterprise'
        THEN 2
    WHEN SEGMENT_HIERARCHY = 'Commercial'
        THEN 3
    WHEN SEGMENT_HIERARCHY = 'Partner'
        THEN 4
    WHEN SEGMENT_HIERARCHY = 'Customer Success 1'
        THEN 5
    WHEN SEGMENT_HIERARCHY = 'Customer Success 2'
        THEN  6
    WHEN SEGMENT_HIERARCHY = 'Customer Success 3'
        THEN 7
    WHEN SEGMENT_HIERARCHY = 'Customer Success 4'
        THEN 8
    WHEN SEGMENT_HIERARCHY = 'Other'
        THEN 9
    ELSE 20 END AS SEGMENT_ROLLUP_ORDER_BY,
  O.CRM_SOURCE,
  O.STAGE,
  O.STAGE_1_DATE_OF_ENTRY,
  O.STAGE_1_DATE_ENTRY,
  CASE
    WHEN O.STAGE = '0 - Qualification'
        THEN '01'
    WHEN O.STAGE = '1 - Discovery' 
        THEN '02'
    WHEN O.STAGE = '2 - Validation'
        THEN '03'
    WHEN O.STAGE = '3 - Justification'
        THEN '04'
    WHEN O.STAGE = '4 - Negotiation'
        THEN '05'
    WHEN O.STAGE = 'Pending Closed Won'
        THEN '06'
    WHEN O.STAGE = 'Closed Won'
        THEN '07'
    WHEN O.STAGE = 'Closed Lost'
        THEN '08'
    ELSE '10' END STAGE_ORDER_BY,
    CASE 
        WHEN O.STAGE IN ('1 - Discovery',
                        '2 - Validation',
                        '3 - Justification',
                        '4 - Negotiation',
                        'Pending Closed Won')
            THEN 'Pipeline'
        WHEN O.STAGE IN ('Closed Won')
            THEN 'Closed Won'
        WHEN O.STAGE IN ('Closed Lost')
            THEN 'Closed Lost'
        ELSE 'Not Provided' END STAGE_CATEGORY,
  O.START_DATE,
  O.SUB_TYPE,
  O.TERM_MONTHS,
  O.TERM_YEARS,
  O.TERRITORY,
  O.TERRITORY_MANAGER_ID,
  O.TERRITORY_MANAGER_NAME,
  O.TCV_NON_RECURRING,
  O.TCV_SERVICES,
  O.TCV_SUBSCRIPTIONS,
  O.TM_SEGMENT_NAME AS TM_SEGMENT,
  O.TOTAL_DEAL_VALUE,
  O.TOTAL_RENEWAL_DUE,
  O.TYPE,			
  CASE
      WHEN O.TYPE IN ('New Business',
                      'New Business - New Logo')
          THEN 'New Logo'
      WHEN O.TYPE = 'New Business - New Product'
          THEN 'New Product'
      WHEN O.TYPE = 'New Business - New Operating Division'
          THEN 'New Operating Division'
      WHEN O.TYPE IN ('Existing Business',
                      'Add-on Subscription')	
          THEN 'Add-on/Expansion'
      ELSE 'Not Provided' END A1_SUB_TYPE,
  O.XOPPORTUNITY_ID AS CROSS_CRM_ID,
  O.X15_DATE,
  C.CONTACT_NAME, 
  C.EMAIL AS CONTACT_EMAIL,
  P.OG_PRODUCT_FAMILY,
  P.PRODUCT_LINE,
  P.PRODUCT_FAMILY,
  P.ALLOCATED_ARR_SUM,
  L.PRODUCT_LIST,
  O."Influenced Partner",
  CASE 
      WHEN O."Influenced Partner" IS NULL 
           THEN 0::BOOLEAN
      ELSE 1::BOOLEAN
    END "Has Influenced Partner",
  W.STAGE AS LAST_WEEK_STAGE,
  W.FORECAST_CATEGORY AS LAST_WEEK_FORECAST_CATEGORY,
  W.CLOSE_BOOKINGS_FISCAL_PERIOD AS LAST_WEEK_CLOSE_FISCAL_PERIOD,
  W.ARR AS LAST_WEEK_ARR,
  CASE
    WHEN O.STAGE = 'Closed Lost' AND O.CLOSED_REASON_CATEGORY = 'Duplicate'
        THEN 'Remove'
    ELSE 'Keep' END LOST_DUPLICATE
FROM APTTUS_DW.SF_PRODUCTION."Opportunity_C2" O

LEFT JOIN APTTUS_DW.SF_PRODUCTION."Contact_C2" C
    ON O.CONTACT_ID = C.CONTACT_ID
    
LEFT JOIN HIGHEST_PRODUCT_FAMILY P
    ON O.OPPORTUNITY_ID = P.OPPORTUNITY_ID
    
LEFT JOIN C2_PRODUCT_LISTS L
    ON O.OPPORTUNITY_ID = L.OPPORTUNITY_ID

LEFT JOIN LAST_WEEK_FIELDS W
    ON O.OPPORTUNITY_ID = W.OPPORTUNITY_ID    
    
WHERE ((CONTAINS(O.BOOKING_STAMP, 'Apttus') OR CONTAINS(O.BOOKING_STAMP, '- Sales')) OR O.SALES_MRR > 0)
    AND IFNULL(LOST_DUPLICATE,'') NOT IN( 'Remove')
    AND IFNULL(OWNER_ROLE,'') NOT IN( 'Program Associate')
    AND IFNULL(SUB_TYPE,'') NOT IN( 'Quoting Opportunity');
   

    
   