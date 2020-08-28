CREATE OR REPLACE VIEW APTTUS_DW.SF_CONGA1_0."Account_C1"  COMMENT = 'all purpose Conga1.0 account view' AS 

with next_oppty_closing as (
        select ACCOUNTID, count(*) as FUTURE_OPPTY_COUNT, MIN(CLOSEDATE) as NEXT_CLOSEDATE
        from APTTUS_DW.SF_CONGA1_0.OPPORTUNITY 
        where STAGENAME NOT IN ('Closed Won', 'Closed Lost', 'Cancellation')
          and CLOSEDATE > CURRENT_DATE 
          and (BILLING_FREQUENCY__C not in ('Consolidated - Annual') or BILLING_FREQUENCY__C is null)
        group by ACCOUNTID 
)

, max_next_oppty_closing as (
        select A.ACCOUNTID, B.NEXT_CLOSEDATE, max(A.ID) as AN_OPPORTUNITY, count(*) as NEXT_OPTTY_COUNT, B.FUTURE_OPPTY_COUNT
        from                  APTTUS_DW.SF_CONGA1_0.OPPORTUNITY A
        inner join            next_oppty_closing B
                     on  A.ACCOUNTID = B.ACCOUNTID
                     and A.CLOSEDATE = B.NEXT_CLOSEDATE
        group by A.ACCOUNTID, B.NEXT_CLOSEDATE, B.FUTURE_OPPTY_COUNT
)

, last_oppty_won as (
        select ACCOUNTID, count(*) as WON_OPPTY_COUNT, MAX(CLOSEDATE) as LAST_CLOSEDATE
        from APTTUS_DW.SF_CONGA1_0.OPPORTUNITY 
        where STAGENAME IN ('Closed Won')
          and CLOSEDATE <= CURRENT_DATE 
          and (BILLING_FREQUENCY__C not in ('Consolidated - Annual') or BILLING_FREQUENCY__C is null)       
        group by ACCOUNTID 
)

, max_last_oppty_won as (
        select A.ACCOUNTID, C.LAST_CLOSEDATE, max(A.ID) as AN_OPPORTUNITY, count(*) as LAST_OPTTY_COUNT, C.WON_OPPTY_COUNT
        from                  APTTUS_DW.SF_CONGA1_0.OPPORTUNITY A
        inner join            last_oppty_won C
                     on  A.ACCOUNTID = C.ACCOUNTID
                     and A.CLOSEDATE = C.LAST_CLOSEDATE
        group by A.ACCOUNTID, C.LAST_CLOSEDATE, C.WON_OPPTY_COUNT
)

, main as (
SELECT A.ACCOUNTID_18__C --, A.id   
       , A.NAME                                                                     AS ACCOUNT_NAME 
       , coalesce(L.DNBOPTIMIZER__DOMESTICULTIMATEBUSINESSNAME__C, 'Not Provided')  AS DNB_DOMESTIC_ULIMATE_BUSINESS_NAME
       , L.DNBOPTIMIZER__DOMESTICULTIMATEBUSINESSNAME__C                            AS DNB_DOMESTIC_ULIMATE_BUSINESS_NAME__N
       , A.DNBOPTIMIZER__DNB_D_U_N_S_NUMBER__C 
       , coalesce(L.DNBOPTIMIZER__DUNSNUMBER__C, -1)                                AS DNB_CHILD_DUNS_NUMBER 
       , L.DNBOPTIMIZER__DUNSNUMBER__C                                              AS DNB_CHILD_DUNS_NUMBER__N 
       , coalesce(L.DNBOPTIMIZER__DOMESTICULTIMATEDUNSNUMBER__C, 'Not Provided')    AS DNB_DUNS_NUMBER -- old_name
       , L.DNBOPTIMIZER__DOMESTICULTIMATEDUNSNUMBER__C                              AS DNB_DUNS_NUMBER__N -- old_name
       , coalesce(L.DNBOPTIMIZER__DOMESTICULTIMATEDUNSNUMBER__C, 'Not Provided')    AS DNB_DOMESTIC_DUNS_NUMBER 
       , L.DNBOPTIMIZER__DOMESTICULTIMATEDUNSNUMBER__C                              AS DNB_DOMESTIC_DUNS_NUMBER__N
       , A.TEST_ACCOUNT__C 
       , coalesce(A.TYPE, 'Not Provided')                                           AS "TYPE"
       , A.TYPE                                                                     AS TYPE__N
       , A.TM_GEOID__C 
       , CASE 
           WHEN A.TM_GEOID__C IS NULL THEN 'Not Provided' 
           ELSE COALESCE(D.NAME, 'Not Found') 
         END                                                                        AS GEO_NAME 
       , D.NAME                                                                     AS GEO_NAME__N        
       , A.TM_REGIONID__C 
       , CASE 
           WHEN A.TM_REGIONID__C IS NULL THEN 'Not Provided' 
           ELSE COALESCE(C.NAME, 'Not Found') 
         END                                                                        AS REGION_NAME 
       , C.NAME                                                                     AS REGION_NAME__N         
       , A.TM_SEGMENTID__C 
       , CASE 
           WHEN A.TM_SEGMENTID__C IS NULL THEN 'Not Provided' 
           ELSE COALESCE(N.NAME, 'Not Found') 
         END                                                                        AS TM_SEGMENT_NAME
       , N.NAME                                                                     AS TM_SEGMENT_NAME__N        
       -- , A.TM_TERRITORY_SEGMENTID__C  deprecated
       , A.TM_Segment_TerritoryId__c
       , CASE 
           WHEN A.TM_Segment_TerritoryId__c IS NULL THEN 'Not Provided' 
           ELSE COALESCE(B.NAME, 'Not Found') 
         END                                                                        AS SEGMENT_TERRITORY_NAME 
       , B.NAME                                                                     AS SEGMENT_TERRITORY_NAME__N 
       -- for compatability SEGMENT_TERRITORY_NAME is also TERRITORY_NAME
       , B.NAME                                                                     AS TERRITORY_NAME 
         -- for compatability
       , A.TM_DIVISIONID__C 
       , CASE 
           WHEN A.TM_DIVISIONID__C IS NULL THEN 'Not Provided' 
           ELSE COALESCE(E.NAME, 'Not Found') 
         END                                                                        AS TM_DIVISION_NAME 
       , E.NAME                                                                     AS TM_DIVISION_NAME__N 
       , A.TM_Division_TerritoryId__c
       , CASE 
           WHEN A.TM_Division_TerritoryId__c IS NULL THEN 'Not Provided' 
           ELSE COALESCE(H.NAME, 'Not Found') 
         END                                                                        AS DIVISION_TERRITORY_NAME 
       , H.NAME                                                                     AS DIVISION_TERRITORY_NAME__N 
       , A.OWNERID 
       , CASE 
           WHEN A.OWNERID IS NULL THEN 'Not Provided' 
           ELSE COALESCE(I.FULL_NAME__C, 'Not Found') 
         END                                                                        AS OWNER_NAME 
       , I.FULL_NAME__C                                                             AS OWNER_NAME__N 
       --, account_owner_id__c --is this different that ownerid ? 15  
       , A.ORIGINAL_OWNER__C 
       , A.OWNERSHIP 
       --, A.owner_email__c  
       , A.OWNER_FULL_NAME__C 
       , A.OWNER_IS_ACTIVE__C 
       , A.CUSTOMER_SUCCESS_MANAGER__C 
       , CASE 
           WHEN A.CUSTOMER_SUCCESS_MANAGER__C IS NULL THEN 'Not Provided' 
           ELSE COALESCE(F.NAME, 'Not Found') 
         END                                                                        AS CUSTOMER_SUCCESS_MANAGER_NAME
       , F.NAME                                                                     AS CUSTOMER_SUCCESS_MANAGER_NAME__N
       , coalesce(A.CS_DIVISION_STAMP__C, 'Not Provided')                           AS CS_DIVISION_STAMP                        
       , A.CS_DIVISION_STAMP__C                                                     AS CS_DIVISION_STAMP__N
       , coalesce(A.CS_TEAM__C, 'Not Provided')                                     AS CS_TEAM
       , A.CS_TEAM__C                                                               AS CS_TEAM__N
       , coalesce(A.TEAM__C, 'Not Provided')                                        AS TEAM                                                                       
       , A.TEAM__C                                                                  AS TEAM__N

       , A.TM_TERRITORY_MANAGERID__C -- , A.territory_manager__c  
       , CASE 
           WHEN A.TM_TERRITORY_MANAGERID__C IS NULL THEN 'Not Provided' 
           ELSE COALESCE(G.FULL_NAME__C, 'Not Found') 
         END                                                                        AS TERRITORY_MANAGER_NAME
       , G.FULL_NAME__C                                                             AS TERRITORY_MANAGER_NAME__N 
       , A.TM_RENEWALS_MANAGERID__C 
       , CASE 
           WHEN A.TM_RENEWALS_MANAGERID__C IS NULL THEN 'Not Provided' 
           ELSE COALESCE(O.FULL_NAME__C, 'Not Found') 
         END                                                                        AS RENEWALS_MANAGER_NAME
       , O.FULL_NAME__C                                                             AS RENEWALS_MANAGER_NAME__N 
       , A.ESTABLISHING_PARTNER__C 
       , CASE 
           WHEN A.ESTABLISHING_PARTNER__C IS NULL THEN 'Not Provided' 
           ELSE COALESCE(M.NAME, 'Not Found') 
         END                                                                        AS ESTABLISHING_PARTNER_NAME
       , M.NAME                                                                     AS ESTABLISHING_PARTNER_NAME__N
       , A.OPEN_OPPORTUNITIES__C 
       , CASE 
           WHEN J.NEXT_OPTTY_COUNT > 0 
             THEN J.AN_OPPORTUNITY 
           ELSE 'Not Found' 
         END                                                                        AS NEXT_OPPORTUNITY 
       , COALESCE(J.NEXT_OPTTY_COUNT, 0)                                            AS NEXT_DUP 
       , COALESCE(J.FUTURE_OPPTY_COUNT, 0)                                          AS FUTURE_OPTTY_COUNT 
       , J.NEXT_CLOSEDATE                                                           AS OPPORTUNITY_NEXT_CLOSEDATE 
       , A.NEXT_RENEWAL__C                                                          AS RENEWAL_DATE
       , CASE 
           WHEN K.LAST_OPTTY_COUNT > 0 THEN K.AN_OPPORTUNITY 
           ELSE 'Not Found' 
         END                                                                        AS LAST_OPPORTUNITY 
       , COALESCE(K.LAST_OPTTY_COUNT, 0)                                            AS LAST_DUP 
       , COALESCE(K.WON_OPPTY_COUNT, 0)                                             AS PAST_WON_COUNT 
       , K.LAST_CLOSEDATE                                                           AS OPPORTUNITY_LAST_CLOSEDATE 
       /* join dnb fields as L.* */ 
       , A.DNBOPTIMIZER__DNBCOMPANYRECORD__C 
       , A.NUMBEROFEMPLOYEES                                 
       , coalesce(A.NUMBEROFEMPLOYEES, -1)                                          AS NUMBER_OF_EMPLOYEES  
       , coalesce(L.DNBOPTIMIZER__EMPLOYEECOUNTTOTAL__C, -1)                        AS DNB_EMPLOYEE_COUNT_TOTAL 
       , CASE
           WHEN A.MRR_ACTIVE_MRR__C is not null
             THEN A.MRR_ACTIVE_MRR__C * 12
          ELSE -1
         END                                                                        AS ANNUAL_REVENUE
       , coalesce(L.DNBOPTIMIZER__SALESVOLUMEUSDOLLARS__C, -1)                      AS DNB_REVENUE 

       , coalesce(L.DNBOPTIMIZER__LOCATIONTYPE__C, 'Not Provided')                  AS DNB_LOCATION_TYPE 
       , L.DNBOPTIMIZER__LOCATIONTYPE__C                                            AS DNB_LOCATION_TYPE__N
       , L.DNBOPTIMIZER__PRIMARYLATITUDE__C                                         AS DNB_LATITUDE
       , L.DNBOPTIMIZER__PRIMARYLONGITUDE__C                                        AS DNB_LONGITUDE
       --, L.DNBOPTIMIZER__GLOBALULTIMATEINDICATOR__C                               as DNB_GLOBAL_ULTIMATE_INDICATOR 
       , L.DNBOPTIMIZER__GLOBALULTIMATEDUNSNUMBER__C                                AS DNB_GLOBAL_ULTIMATE_DUNS_NUMBER 
       , coalesce(L.DNBOPTIMIZER__GLOBALULTIMATEBUSINESSNAME__C, 'Not Provided')    AS DNB_GLOBAL_ULTIMATE_BUSINESS_NAME -- new
       , L.DNBOPTIMIZER__GLOBALULTIMATEBUSINESSNAME__C                              AS DNB_GLOBAL_ULTIMATE_BUSINESS_NAME__N -- new       
       , L.DNBOPTIMIZER__PARENTBUSINESSNAME__C                                      AS DNB_PARENT_BUSINESS_NAME 
       , L.DNBOPTIMIZER__PARENTDUNSNUMBER__C                                        AS DNB_PARENT_DUNS_NUMBER 
       , A.BILLINGSTREET                                                            AS BILLING_STREET
       , L.DNBOPTIMIZER__PRIMARYSTREETADDRESS__C                                    AS DNB_BILLING_STREET 
       , A.SHIPPINGSTREET                                                           AS SHIPPING_STREET
       , A.BILLINGCITY                                                              AS BILLING_CITY
       , L.DNBOPTIMIZER__PRIMARYCITYNAME__C                                         AS DNB_BILLING_CITY
       , A.SHIPPINGCITY                                                             AS SHIPPING_CITY
       , A.BILLINGSTATE                                                             AS BILLING_STATE
       , L.DNBOPTIMIZER__PRIMARYSTATEPROVINCENAME__C                                AS DNB_PRIMARYSTATEPROVINCENAME
       , L.DNBOPTIMIZER__PRIMARYSTATEPROVINCEABBREVIATION__C                        AS DNB_STATE_PROVINCE_ABBREVIATION
       , A.SHIPPINGSTATE                                                            AS SHIPPING_STATE
       , A.BILLINGCOUNTRY                                                           AS BILLING_COUNTRY
       , L.DNBOPTIMIZER__PRIMARYCOUNTRYCODE__C                                      AS DNB_COUNTRY_CODE 
       , A.SHIPPINGCOUNTRY                                                          AS SHIPPING_COUNTRY
       , A.BILLINGPOSTALCODE                                                        AS BILLING_ZIP_POSTAL_CODE
       , L.DNBOPTIMIZER__PRIMARYPOSTALCODE__C                                       AS DNB_ZIP_POSTAL_CODE
       , A.SHIPPINGPOSTALCODE                                                       AS SHIPPING_ZIP_POSTAL_CODE
       , A.BILLINGCONTACT__C                                                        AS BILLING_CONTACT
       , A.BILLING_PARTNER__C                                                       AS BILLING_PARTNER
       , A.BILLING_PARTY_ACCOUNT_ID__C                                              AS BILLING_PARTNER_ACCOUNT_ID 
       , A.BILLING_PARTY__C                                                         AS BILLING_PARTY

       , coalesce(L.DNBOPTIMIZER__NUMBEROFFAMILYMEMBERS__C, -1)                     AS DNB_NUMBER_OF_FAMILY_MEMBERS
       , L.DNBOPTIMIZER__NUMBEROFFAMILYMEMBERS__C                                   AS DNB_NUMBER_OF_FAMILY_MEMBERS__N 
       -- MRR and revenue
       , A.MONTHLY_RECURRING_REVENUE__C 
       , A.BURDENED_MRR__C 
       , A.MRR_ACTIVE_MRR__C 
       , CASE
           WHEN A.MRR_ACTIVE_MRR__C is not null
             THEN A.MRR_ACTIVE_MRR__C * 12
          ELSE 0   
         END                                                                        AS ACV
       , A.MRR_ACTIVE_MRR__C * 12                                                   AS ACV__N  
       , A.MRR_CONTRACTS_MRR__C 
       , A.MRR_DATA_MRR__C 
       , A.MRR_DOCMERGE_MRR__C 
       , A.MRR_ESIGNATURE_MRR__C 
       , A.MRR_RESELLER_MRR__C 
       , A.MRR_SERVICES_MRR__C 
       , A.MRR_WORKFLOW_MRR__C 
       , A.CUSTOMER_SINCE__C                                                        AS CUSTOMER_SINCE_DATE                               
       , coalesce(A.INDUSTRY, 'Not Provided')                                       AS INDUSTRY
       , A.INDUSTRY                                                                 AS INDUSTRY__N    
       , A.WEBSITE        
       , L.DNBOPTIMIZER__WEBADDRESS__C                                              AS DNB_WEB_ADDRESS
       , A.MINTIGO_HASH__C 
       , A.MINTIGO_TARGETING_PRIORITY__C 
       , A.MINTIGO_UPDATE_DATETIME__C
       , coalesce(L.DNBOPTIMIZER__SIC4CODE1DESCRIPTION__C, 'Not Provided')          AS DNB_SIC4_CODE1 
       , A.SIC 
       , A.SICDESC 
       , A.CLOSED_BEFORE_SF_DEAL__C  
       , A.USE_CASES__C 
       , A.APTTUS_CUSTOMER__C 
       , A.TRIAL_EXPIRATION__C 
       , A.TRIAL_PRODUCT__C
       , 'Not Provided'                                                              AS NETSUITE_ID -- new
       , null                                                                        AS NETSUITE_ID__N -- new     
       , coalesce(L.DNBOPTIMIZER__OUTOFBUSINESSINDICATOR__C, 'Not Provided')         AS DNB_OUTOFBUSINESSINDICATOR --new
       , L.DNBOPTIMIZER__OUTOFBUSINESSINDICATOR__C                                   AS DNB_OUTOFBUSINESSINDICATOR__N --new
       , coalesce(L.DNBOPTIMIZER__HEADQUARTERDUNSNUMBER__C, 'Not Provided')          AS DNB_HQ_DUNS_NUMBER -- new
       , L.DNBOPTIMIZER__HEADQUARTERDUNSNUMBER__C                                    AS DNB_HQ_DUNS_NUMBER__N -- new
       , coalesce(L.DNBOPTIMIZER__HEADQUARTERBUSINESSNAME__C, 'Not Provided')        AS DNB_HQ_BUSINESS_NAME -- new
       , L.DNBOPTIMIZER__HEADQUARTERBUSINESSNAME__C                                  AS DNB_HQ_BUSINESS_NAME__N -- new     
       , A.HAVE_SALESFORCE_MINTIGO__C    
       -- significant_dates 
       , A.CREATEDDATE 
       , A.CHURN_DATE__C 
       -- everything else 
       , A.ACCOUNTSOURCE 
       , A.ACCOUNT_COUNT__C 
       , A.ACCOUNT_DELINQUENT_FLAG__C 
       , A.ACCOUNT_EXPIRATION_DATE_WFR__C 
       , A.ACCOUNT_GRADE__C 
       , A.ACCOUNT_OWNER_ROLE__C 
       , A.ACCOUNT_RANK__C 
       , A.ACTIVE_ACCOUNTS_14_DAYS__C 
       , A.ACTIVE_PARTNER_PROFILE_RECORD_ID__C 
       , A.ACTIVE_PARTNER_PROFILE__C 
       , A.AE_HOLD_DATE__C 
       , A.AE_HOLD__C 
       , A.ANNUALREVENUE 
       , A.APTTUS_ACCOUNT_PRIORITY__C 
       , A.APTTUS_PROSPECT__C 
       , A.AVA_SFCORE__EXEMPTENTITYTYPE__C 

       , A.CANNOT_RECEIVE_GIFTS__C 
       , A.CA_SPLIT__C 
       , A.CN_WF_TROUBLESHOOTING__C 
       , A.COLLABORATE_ACCOUNT_ID__C 
       , A.COLLABORATE_ACCOUNT_URL__C 
       , A.COMPETITOR_PARTNERSHIPS__C 
       , A.COMPOSER_SAVINGS__C 
       , A.CONGA_ACCOUNT_BEING_CREATED__C 
       , A.CONGA_CHAMPION__C 
       , A.CONGA_COLLABORATE_TEMPLATE__C 
       , A.CONGA_SOLUTIONS__C 
       , A.COUNTRY__C 
       , A.COUNT_ACTIVE_AND_FUTURE_ASSETS__C 
       , A.COUNT_DELINQUENT_SALES_INVOICES__C 
       , A.COUNT_EXPIRED_ASSETS__C 
       , A.COUNT__C 
       , A.CPQ_CONTRACT_IMPORT_STATUS__C 
       , A.CSM_COMMERCIAL_TERMS__C 
       , A.CSM_INTRODUCTION__C 
       , A.CSM_REPORT_FILTER__C 
       , A.CURRENT_IMPLEMENTATION_PARTNERID__C 
       , A.CURRENT_RESELLER__C 
       , A.CURRENT_YEAR_ACV__C 
       , A.CUSTOMER_HEALTH_FX__C 
       , A.CUSTOMER_HEALTH_TOTAL_FX__C 
       , A.CUSTOMER_LIKELY_TO_SCORE__C 
       , A.CUSTOMER_LIKELY_TO__C 
       , A.CUSTOMER_NEEDS__C 
       , A.CUSTOMER_SENTIMENT_SCORE__C 
       , A.CUSTOMER_SENTIMENT__C 
       , A.CUSTOMER_SINCE2__C 
       , A.CUSTOMER_SINCE_ALL__C 
       , A.CUSTOMER_SINCE_DATE_FORMULA__C 
       , A.CUSTOMER_STATUS__C 
       , A.CUSTOMER_SUCCCESS_MANAGER_EMAIL__C 
       , A.CUSTOMER_SUCCESS__C 
       , A.CXG_ASSET_START_DATE__C 
       , A.DATA_PROCESSING_ADDENDUM__C 
       , A.DATA_QUALITY_DESCRIPTION__C 
       , A.DATA_QUALITY_SCORE__C 
       , A.DATA_RESIDENCY_REQUIREMENT__C 
       , A.DATE_OF_LAST_WON_DEAL__C 
       , A.DESCRIPTION 
       , A.EBR_SCHEDULED_DATE__C 
       , A.EBR_STATUS__C 
       , A.EMAIL_DOMAINS_FORMATED__C 
       , A.EMPLOYEECOUNTESTIMATE__C 
       , A.EMPLOYEE_BAND__C 
       , A.ENGAGIO__STATUS__C 
       , A.ENTITY_CODE__C 
       , A.EXPIRATION_DATE_CHECK_CC__C 
       , A.EXPIRATION_DATE_CHECK__C 
       , A.FAX 
       , A.FIRST_WON_DEAL__C 
       , A.GOLD_LIST_ACCOUNTS__C 
       , A.GROUP__C 
       , A.HANDLE_WITH_CARE_COMMENT__C 
       , A.HANDLE_WITH_CARE__C 
       , A.HAS_PROJECTS__C 
       , A.HAVE_DOCUSIGN__C 
       , A.HAVE_SALESFORCE__C 
       , A.HEALTH_SCORE_TEMPLATE_IMAGE_URL__C 
       , A.IA_CRM__INTACCTID__C 
       , A.IA_CRM__INTACCT_SYNC_ERRORS__C 
       , A.IA_CRM__INTACCT_SYNC_STATUS_IMAGE__C 
       , A.IA_CRM__INTACCT_SYNC_STATUS__C 
       , A.IA_CRM__SYNCED_WITH_INTACCT_DATE__C 
       , A.IA_CRM__SYNC_WITH_INTACCT__C 
       , A.INTENT_AUTOMATED_CONTRACT_MANAGEMENT__C 
       , A.INTENT_DOCUMENT_AUTOMATION__C 
       , A.INVOICING_METHOD__C 
       , A.ISCUSTOMERPORTAL 
       , A.ISDELETED 
       , A.ISLEAD__C 
       , A.ISPARTNER 
       , A.IVP_SEGMENT__C 
       , A.JCTAG__C 
       , A.JIGSAW 
       , A.JIGSAWCOMPANYID 
       , A.LAST_LOST_DEAL_DATE__C 
       , A.LAST_WON_DEAL_DATE__C 
       , A.LAST_WON_RENEWAL_OR_NB__C 
       , A.LEGACY_BILLING_FREQUENCY__C 
       , A.LEGACY_DESCRIPTION__C 
       , A.LEGACY_PAYMENT_METHOD__C 
       , A.LEGAL_HOLD_FLAG__C 
       , A.LICENSE_UTILIZATION_SCORE__C 
       , A.LICENSE_UTILIZATION__C 
       , A.LID__LINKEDIN_COMPANY_ID__C 
       , A.LIFETIME_APXT_REVENUE__C 
       , A.LOGO_ON_WEBSITE__C 
       , A.MASTERRECORDID 
       , A.MONTHS_ELAPSED__C 
       , A.MSA_CONTRACT__C 
       , A.MSA_EXISTS__C 
       , A.NA_TERRITORY__C 
       , A.NOVATUS_ACCOUNT_ID__C 
       , A.NOVATUS_CONDUCTOR_FX__C 
       , A.NOVATUS_CREATEDDATE__C 
       , A.OCTIV_ACCOUNT_ID__C 
       , A.OF_OPEN_IMPLEMENTATIONS__C 
       , A.OF_REFERENCE_CONTACTS__C 
       , A.OLD_TCV_TIER__C 
       , A.PARENTID 
       , A.PARENT_ACCOUNT_BILLING_COUNTRY__C 
       , A.PARENT_ACCOUNT_BILLING_STATE__C 
       , A.PARENT_ACCOUNT_EMPLOYEE_BAND__C 
       , A.PARENT_ACCOUNT_TERRITORY__C 
       , A.PARTNER_INDUSTRIES__C 
       , A.PARTNER_PRESENCE__C 
       , A.PARTNER_STATUS__C 
       , A.PARTNER_TECH_SPECIALIZATIONS__C 
       , A.PHONE 
       , A.POO__C 
       , A.PORTAL_PLATFORM_COMMUNITY_LICENSES__C 
       , A.PRIMARY_ADOPTION_BLOCKER__C 
       , A.PRODUCT_PURCHASE_RESTRICTION__C 
       , A.PURCHASING_METHOD__C 
       , A.R180__C 
       , A.R60__C 
       , A.RATING 
       , A.REASON_FOR_RISK__C 
       , A.RECORDTYPEID 
       , A.REFERENCEABLE_CUSTOMER__C 
       , A.REFERRAL_FEE_PERCENTAGE__C 
       , A.RENEWAL_BUFFER_DAYS__C 
       , A.RENEWAL_PROCESS_DATE__C 
       , A.RENEWAL_UPLIFT_CAP__C 
       , A.ROLL_UP_PARTNER_PROFILE_COUNT__C 
       , A.RR__C 
       , A.RVPE__RVACCOUNT__C 
       , A.SALESFORCE_ACCOUNT_ID__C 
       , A.SALESFORCE_ACCOUNT_USERS__C 
       , A.SALESFORCE_INSTANCE__C 
       , A.SALESFORCE_ORG_ID_15__C 
       , A.SALESFORCE_USERS__C 
       , A.SDR_ASSIGNED__C 
       , A.SECONDARY_ADOPTION_BLOCKER__C 
       , A.SECURITY_NOTICE__C 
       , A.SERVICE_LEVEL_CREDIT_OBLIGATION__C 
       , A.SF_USERS_TIER__C 
       , A.SITE 
       , A.SPECIAL_DATA_BREACH_NOTIFICATIONS__C 
       , A.SPECIAL_LEGAL_TERMS__C 
       , A.SUBSCRIBER_ORG_TYPE__C 
       , A.SUBSCRIPTION_DAYS__C 
       , A.SUBSCRIPTION_END_DATE_FOR_FORMULA__C 
       , A.SUBSCRIPTION_END_DATE__C 
       , A.SUBSCRIPTION_MONTHS_CM__C 
       , A.SUBSCRIPTION_YEARS_CM__C 
       , A.SUM_OF_ADDITIONAL_SUPPORT_HOURS__C 
       , A.SUM_OF_SUPPORT_HOURS__C 
       , A.SUPPORT_HOURS__C 
       , A.SUPPORT_LEVEL__C 
       , A.SUPPORT_LOCATION_RESTRICTION__C 
       , A.SUPPORT_OVERAGE__C 
       , A.SUPPORT_USAGE__C 
       , A.TAG__C 
       , A.TARGET_ACCOUNT__C 
       , A.TERRITORY_MANAGER_ROLE__C 
       , A.TERRITORY_MANAGER_TARGET__C 
       , A.THRESHOLD_CN_WF__C 
       , A.THRESHOLD_COMPOSER_SERVICE_EVENTS__C 
       , A.THRESHOLD_COURIER__C 
       , A.TICKERSYMBOL 
       , A.TIER__C 
       , A.TM_ACCOUNT_HOLD__C 
       , A.TM_REPORT_FILTER__C 
       , A.TOTAL_AGGREGATE_SEATS__C 
       , A.TOTAL_CN_WF_CURRENT_SUBS__C 
       , A.TOTAL_COMPOSER_SERV_EVENT_CURRENT_SUBS__C 
       , A.TOTAL_COURIER_CURRENT_SUBS__C 
       , A.TOTAL_VALUES_OF_CURRENT_SUBSCRIPTION__C 
       , A.URL_LINK__C 
       , A.URL_RAW__C 
       , A.US_INTERNATIONAL__C 
       , A.US_TIME_ZONE__C 
       , A.VALIDATION_RULE_CONDUCTOR_URL__C 
       , A.WEBSITE_LOGOS_CONDUCTOR_URL__C 
--       , A.p2s_base1__attributabledata__c  
--       , A.p2s_base1__attributableevent__c  
--       , A.p2s_base1__daysspent__c  
--       , A.p2s_base1__lifecyclestage__c  
--       , A.p2s_base1__previoustransitiondatetime__c  
--       , A.p2s_base1__transitiondatetime__c  
--       , A.zisf__zoominfo_industry__c  
--       , A.zisf__zoom_id__c  
--       , A.zisf__zoom_lastupdated__c  
-- contral fields 
--       , A.lastactivitydate  
--       , A.lastmodifiedbyid  
--       , A.lastmodifieddate  
--       , A.systemmodstamp  
--       , A._sdc_batched_at  
--       , A._sdc_extracted_at  
--       , A._sdc_received_at  
--       , A._sdc_sequence  
--       , A._sdc_table_version  
FROM   APTTUS_DW.SF_CONGA1_0.ACCOUNT A 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.TM_TERRITORY__C B 
                    ON A.TM_Segment_TerritoryId__c = B.ID 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.TM_TERRITORY__C H 
                    ON A.TM_Segment_TerritoryId__c = H.ID                     
                    
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.TM_REGION__C C 
                    ON A.TM_REGIONID__C = C.ID 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.TM_GEO__C D 
                    ON A.TM_GEOID__C = D.ID 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.TM_DIVISION__C E 
                    ON A.TM_DIVISIONID__C = E.ID 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.USER F 
                    ON A.CUSTOMER_SUCCESS_MANAGER__C = F.ID 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.USER G 
                    ON A.TM_TERRITORY_MANAGERID__C = G.ID 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.USER I 
                    ON A.OWNERID = I.ID 
       LEFT OUTER JOIN MAX_NEXT_OPPTY_CLOSING J 
                    ON A.ID = J.ACCOUNTID 
       LEFT OUTER JOIN MAX_LAST_OPPTY_WON K 
                    ON A.ID = K.ACCOUNTID 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.DNBOPTIMIZER__DNBCOMPANYRECORD__C L 
                    ON A.DNBOPTIMIZER__DNBCOMPANYRECORD__C = L.ID 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.ACCOUNT M 
                    ON A.ESTABLISHING_PARTNER__C = M.ACCOUNTID_18__C 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.TM_SEGMENT__C N 
                    ON A.TM_SEGMENTID__C = N.ID 
       LEFT OUTER JOIN APTTUS_DW.SF_CONGA1_0.USER O 
                    ON A.TM_RENEWALS_MANAGERID__C = O.ID                   
WHERE A.ISDELETED = FALSE
                   
)

select * from main
;

SHOW GRANTS ON APTTUS_DW.SF_CONGA1_0."Account_C1";
GRANT SELECT ON APTTUS_DW.SF_CONGA1_0."Account_C1" TO SYSADMIN with GRANT OPTION;
SHOW GRANTS ON APTTUS_DW.SF_CONGA1_0."Account_C1"





