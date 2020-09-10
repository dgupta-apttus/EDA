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
	FROM APTTUS_DW.SF_CONGA1_0."FMA_Feature_Daily_Values"   
	WHERE SFLMA__PACKAGE__C IN ('a015000000xCYUxAAO','a015000000u3HKfAAM','a0150000017jcZWAAY')
      AND SFFMA__FULLNAME__C IN ('APXT_Redlining__Send_for_Negotiation_Events'
                                 ,'FSTR__Total_Tasks_90'
                                 ,'APXT_Redlining__Monthly_Active_Users'
                                 ,'CRMC_PP__Views_Loaded'
                                 ,'APXT_Redlining__View_Redlines_Page_Events'
                                 ,'CRMC_PP__Monthly_Active_Users'
                                 ,'FSTR__Distinct_Users_30')
)     
	SELECT *
	FROM reduce
	pivot(sum(FEATURE_VALUE) for SFFMA__FULLNAME__C in ('CRMC_PP__Views_Loaded','CRMC_PP__Monthly_Active_Users'))
      as p (SOURCE_ORG_ID
	     , LICENSE_ID
         , SFLMA__PACKAGE__C
         , PRODUCT
         , FEATURE_STATUS
         , ACTIVITY_DATE
         , ACTIVITY_COUNT
         , MONTHLY_ACTIVE_USERS
         )
    WHERE SFLMA__PACKAGE__C = 'a015000000u3HKfAAM' 
    ORDER BY 1, 2, 6
    

;	