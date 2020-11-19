--SFFMA__PACKAGE__C  ID                 SFFMA__FULLNAME__C                          PACKAGE_NAMEFX__C PRODUCT      
------------------ ------------------ ------------------------------------------- ----------------- ------------ 
--a015000000xCYUxAAO a9v1T000000t0OcQAI APXT_Redlining__Send_for_Negotiation_Events Conga Contracts   Contracts4SF 
--a015000000xCYUxAAO a9v1T000000t0ObQAI APXT_Redlining__Monthly_Active_Users        Conga Contracts   Contracts4SF 
--a015000000xCYUxAAO a9v1T000000t0OdQAI APXT_Redlining__View_Redlines_Page_Events   Conga Contracts   Contracts4SF 
--a015000000u3HKfAAM a9v1T000000Yez8QAC CRMC_PP__Views_Loaded                       ActionGrid        Grid         
--a015000000u3HKfAAM a9v1T000000sbLeQAI CRMC_PP__Monthly_Active_Users               ActionGrid        Grid         
--a0150000017jcZWAAY a9v500000001bphAAA FSTR__Total_Tasks_90                        ProcessComposer   Orchestrate  
--a0150000017jcZWAAY a9v1T000000saqWQAQ FSTR__Distinct_Users_30                     ProcessComposer   Orchestrate  
  

select * 
  from APTTUS_DW.SF_CONGA1_0."FMA_Feature_Daily_Values"
    pivot(sum(FEATURE_VALUE) for SFFMA__FULLNAME__C in ('APXT_Redlining__Monthly_Active_Users'))
      as p
;

SELECT LICENSE_ID, SFLMA__PACKAGE__C, PRODUCT, SFFMA__FEATUREPARAMETER__C, FEATURE_STATUS, ACTIVITY_DATE, CUSTOMER_ORG_ID__C
FROM APTTUS_DW.SF_CONGA1_0."FMA_Feature_Daily_Values"
    pivot(sum(FEATURE_VALUE) for SFFMA__FULLNAME__C in ('APXT_Redlining__Monthly_Active_Users'))
      as p (LICENSE_ID, SFLMA__PACKAGE__C, PRODUCT, SFFMA__FEATUREPARAMETER__C, FEATURE_STATUS, ACTIVITY_DATE, CUSTOMER_ORG_ID__C, APXT_Redlining__Monthly_Active_Users) 
     ;


    
WITH reduce AS (
	SELECT CUSTOMER_ORG_ID__C AS SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT
         , FEATURE_STATUS
         , ACTIVITY_DATE
         , SFFMA__FULLNAME__C
         , FEATURE_VALUE
	FROM APTTUS_DW.PRODUCT."FMA_Feature_Daily_Values"   
	WHERE SFLMA__PACKAGE__C IN ('a015000000xCYUxAAO')
      AND SFFMA__FULLNAME__C IN ( 'APXT_Redlining__Send_for_Negotiation_Events'
                                 ,'APXT_Redlining__View_Redlines_Page_Events'
                                 )
      AND ACTIVITY_DATE > '2020-09-01'                           
)    
;


WITH reduce AS (
	SELECT CUSTOMER_ORG_ID__C AS SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT
         , FEATURE_STATUS
         , ACTIVITY_DATE
         , SFFMA__FULLNAME__C
         , FEATURE_VALUE
	FROM APTTUS_DW.PRODUCT."FMA_Feature_Daily_Values"   
	WHERE SFLMA__PACKAGE__C IN ('a015000000u3HKfAAM','a0150000017jcZWAAY','a015000000xCYUxAAO')
      AND SFFMA__FULLNAME__C IN ( 'FSTR__Total_Tasks_90'
                                 ,'FSTR__Distinct_Users_30'  
                                 ,'CRMC_PP__Views_Loaded'
                                 ,'CRMC_PP__Monthly_Active_Users'
                                 ,'APXT_Redlining__Monthly_Active_Users'
                                 ,'APXT_Redlining__Send_for_Negotiation_Events'
                                 ,'APXT_Redlining__View_Redlines_Page_Events'
                                 )
      AND ACTIVITY_DATE > '2020-09-01' 
)  
, pivot_contract_events AS ( 
	SELECT *
	FROM reduce
	pivot(sum(FEATURE_VALUE) for SFFMA__FULLNAME__C in ('APXT_Redlining__Send_for_Negotiation_Events','APXT_Redlining__View_Redlines_Page_Events'))
      as p (SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT
         , FEATURE_STATUS
         , ACTIVITY_DATE
         , SEND_FOR_NEGOTIATION
         , VIEW_REDLINES_PAGE
         )
    WHERE SFLMA__PACKAGE__C IN ('a015000000xCYUxAAO') 
)
, calculate_lags AS (
    SELECT SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT
         , FEATURE_STATUS        
         , ACTIVITY_DATE   
         , SEND_FOR_NEGOTIATION 
         , coalesce(lag(SEND_FOR_NEGOTIATION,1) over (partition by SOURCE_ORG_ID order by SOURCE_ORG_ID, ACTIVITY_DATE),0) as PREV_SEND_FOR_NEGOTIATION      
         , VIEW_REDLINES_PAGE
         , coalesce(lag(VIEW_REDLINES_PAGE,1) over (partition by SOURCE_ORG_ID order by SOURCE_ORG_ID, ACTIVITY_DATE),0) AS PREV_VIEW_REDLINES_PAGE
    from pivot_contract_events     
)
, contract_events_daily AS (
    SELECT SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT
         , FEATURE_STATUS        
         , ACTIVITY_DATE
         , case 
                 when SEND_FOR_NEGOTIATION >= PREV_SEND_FOR_NEGOTIATION
                   then (SEND_FOR_NEGOTIATION - PREV_SEND_FOR_NEGOTIATION) 
                 else SEND_FOR_NEGOTIATION   
           end AS SEND_FOR_NEGOTIATION_EVENTS     
         , case
                 when VIEW_REDLINES_PAGE >= PREV_VIEW_REDLINES_PAGE
                   then (VIEW_REDLINES_PAGE - PREV_VIEW_REDLINES_PAGE) 
                 else VIEW_REDLINES_PAGE  
           end as VIEW_REDLINES_PAGE_EVENTS         
    FROM calculate_lags 
)


;



/*
	SELECT *
	FROM reduce
	pivot(sum(FEATURE_VALUE) for SFFMA__FULLNAME__C in ('CRMC_PP__Views_Loaded','CRMC_PP__Monthly_Active_Users'))
      as p (SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT
         , FEATURE_STATUS
         , ACTIVITY_DATE
         , ROLLING_ACTIVITY_COUNT
         , ROLLING_ACTIVE_USERS
         )
    WHERE SFLMA__PACKAGE__C = 'a015000000u3HKfAAM' 
UNION
	SELECT *
	FROM reduce
	pivot(sum(FEATURE_VALUE) for SFFMA__FULLNAME__C in ('FSTR__Total_Tasks_90','FSTR__Distinct_Users_30'))
      as p (SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT
         , FEATURE_STATUS
         , ACTIVITY_DATE
         , ROLLING_ACTIVITY_COUNT
         , ROLLING_ACTIVE_USERS
         )
    WHERE SFLMA__PACKAGE__C = 'a0150000017jcZWAAY'     
*/
;	