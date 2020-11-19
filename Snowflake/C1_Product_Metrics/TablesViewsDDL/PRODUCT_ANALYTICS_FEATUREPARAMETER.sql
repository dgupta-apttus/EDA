
CREATE TABLE APTTUS_DW.PRODUCT.PRODUCT_ANALYTICS_FEATUREPARAMETER 
  ( 
     SFFMA__PACKAGE__C                      VARCHAR(16777216) 
     , ID                                   VARCHAR(16777216) 
     , NAME                                 VARCHAR(16777216) 
     , SFFMA__FULLNAME__C                   VARCHAR(16777216) 
     , SFFMA__INTRODUCEDINPACKAGEVERSION__C VARCHAR(16777216) 
     , PACKAGE_NAMEFX__C                    VARCHAR(15) 
     , PRODUCT                              VARCHAR(12) 
     , TRACK_START_DATE                     DATE 
     , TRACK_END_DATE                       DATE 
     , C1_MONTHLY_COLLECTION                BOOLEAN 
     , C2_DAILY_COLLECTION                  BOOLEAN 
     , C2_DAILY_REPORTING                   BOOLEAN 
     , C1_ADOPTION_USE                      BOOLEAN 
     , PRODUCT_LINE                         VARCHAR(255)
  ); 

update APTTUS_DW.PRODUCT.PRODUCT_ANALYTICS_FEATUREPARAMETER
  set PRODUCT_LINE = 'Conga Orchestrate'
where PRODUCT = 'Orchestrate'
;    
--ALTER TABLE APTTUS_DW.PRODUCT.PRODUCT_ANALYTICS_FEATUREPARAMETER ADD COLUMN  PRODUCT_LINE VARCHAR(255); 

update APTTUS_DW.PRODUCT.PRODUCT_ANALYTICS_FEATUREPARAMETER
  set C1_ADOPTION_USE = true
where sfFma__FullName__c in ('FSTR__Total_Tasks_90','FSTR__Distinct_Users_30','CRMC_PP__Views_Loaded', 'CRMC_PP__Monthly_Active_Users'
  , 'APXT_Redlining__Monthly_Active_Users','APXT_Redlining__Send_for_Negotiation_Events'
  , 'APXT_Redlining__View_Redlines_Page_Events')
;  

-- original create
create table APTTUS_DW.PRODUCT.PRODUCT_ANALYTICS_FEATUREPARAMETER
  AS
select SFFMA__PACKAGE__C, ID, NAME, SFFMA__FULLNAME__C, SFFMA__INTRODUCEDINPACKAGEVERSION__C 
     , CASE WHEN SFFMA__PACKAGE__C = 'a015000000u3HKfAAM' THEN 'ActionGrid' 
            WHEN SFFMA__PACKAGE__C = 'a015000000xCYUxAAO' THEN 'Conga Contracts'
            WHEN SFFMA__PACKAGE__C = 'a0150000017jcZWAAY' THEN 'ProcessComposer'
        ELSE 'Unknown'
       END AS PACKAGE_NAMEFX__C    
     , CASE WHEN SFFMA__PACKAGE__C = 'a015000000u3HKfAAM' THEN 'Grid' 
            WHEN SFFMA__PACKAGE__C = 'a015000000xCYUxAAO' THEN 'Contracts4SF'
            WHEN SFFMA__PACKAGE__C = 'a0150000017jcZWAAY' THEN 'Orchestrate'
        ELSE 'Unknown'
       END AS PRODUCT 
     , CASE WHEN sfFma__FullName__c in ('CRMC_PP__Average_Page_Rows', 'CRMC_PP__Conditional_Formatting_Rules', 'CRMC_PP__Maximum_Page_Rows', 'CRMC_PP__Max_Filter_Levels'
  , 'CRMC_PP__Max_Mass_Create_Records', 'CRMC_PP__Max_Mass_Update_Records', 'CRMC_PP__Monthly_Active_Users', 'CRMC_PP__Unique_Objects', 'CRMC_PP__Views'
  , 'CRMC_PP__Visualforce_Embedded_Pages', 'CRMC_PP__Visualforce_Tabs', 'CRMC_PP__Save_View_Clicks', 'CRMC_PP__Save_Data_Clicks', 'CRMC_PP__Add_Record_Clicks'
  , 'CRMC_PP__Views_Loaded', 'FSTR__Distinct_Users','FSTR__Distinct_Users_30','FSTR__Distinct_Users_90','FSTR__Total_Approval_Steps','FSTR__Total_Checklists'
  , 'FSTR__Total_Checklist_Items','FSTR__Total_Definitions','FSTR__Total_Delayed_Steps','FSTR__Total_Dependencies','FSTR__Total_Document_Generation'
  , 'FSTR__Total_Email_Alerts','FSTR__Total_Event_Placeholders','FSTR__Total_Field_Updates','FSTR__Total_Field_Update_Steps','FSTR__Total_Initiators'
  , 'FSTR__Total_Initiator_Criteria','FSTR__Total_Loop_Back_Steps','FSTR__Total_PCE_Steps','FSTR__Total_Tasks','FSTR__Total_Tasks_90','FSTR__Total_Validations'
  , 'FSTR__Total_Webservice_Callouts','FSTR__User_Count', 'APXT_Redlining__Monthly_Active_Users','APXT_Redlining__Send_for_Negotiation_Events'
  , 'APXT_Redlining__View_Redlines_Page_Events')  
               THEN '1970-01-01'::date 
        else '2020-08-17'::date
       END as TRACK_START_DATE          
     , null::date as TRACK_END_DATE 
     , CASE WHEN sfFma__FullName__c in ('CRMC_PP__Average_Page_Rows', 'CRMC_PP__Conditional_Formatting_Rules', 'CRMC_PP__Maximum_Page_Rows', 'CRMC_PP__Max_Filter_Levels'
  , 'CRMC_PP__Max_Mass_Create_Records', 'CRMC_PP__Max_Mass_Update_Records', 'CRMC_PP__Monthly_Active_Users', 'CRMC_PP__Unique_Objects', 'CRMC_PP__Views'
  , 'CRMC_PP__Visualforce_Embedded_Pages', 'CRMC_PP__Visualforce_Tabs', 'CRMC_PP__Save_View_Clicks', 'CRMC_PP__Save_Data_Clicks', 'CRMC_PP__Add_Record_Clicks'
  , 'CRMC_PP__Views_Loaded', 'FSTR__Distinct_Users','FSTR__Distinct_Users_30','FSTR__Distinct_Users_90','FSTR__Total_Approval_Steps','FSTR__Total_Checklists'
  , 'FSTR__Total_Checklist_Items','FSTR__Total_Definitions','FSTR__Total_Delayed_Steps','FSTR__Total_Dependencies','FSTR__Total_Document_Generation'
  , 'FSTR__Total_Email_Alerts','FSTR__Total_Event_Placeholders','FSTR__Total_Field_Updates','FSTR__Total_Field_Update_Steps','FSTR__Total_Initiators'
  , 'FSTR__Total_Initiator_Criteria','FSTR__Total_Loop_Back_Steps','FSTR__Total_PCE_Steps','FSTR__Total_Tasks','FSTR__Total_Tasks_90','FSTR__Total_Validations'
  , 'FSTR__Total_Webservice_Callouts','FSTR__User_Count', 'APXT_Redlining__Monthly_Active_Users','APXT_Redlining__Send_for_Negotiation_Events'
  , 'APXT_Redlining__View_Redlines_Page_Events')  
               THEN 1::boolean
        else 0::boolean
       end as C1_MONTHLY_COLLECTION 
     , 1::boolean as C2_DAILY_COLLECTION  
     , CASE WHEN sfFma__FullName__c in ('CRMC_PP__Average_Page_Rows', 'CRMC_PP__Conditional_Formatting_Rules', 'CRMC_PP__Maximum_Page_Rows', 'CRMC_PP__Max_Filter_Levels'
  , 'CRMC_PP__Max_Mass_Create_Records', 'CRMC_PP__Max_Mass_Update_Records', 'CRMC_PP__Monthly_Active_Users', 'CRMC_PP__Unique_Objects', 'CRMC_PP__Views'
  , 'CRMC_PP__Visualforce_Embedded_Pages', 'CRMC_PP__Visualforce_Tabs', 'CRMC_PP__Save_View_Clicks', 'CRMC_PP__Save_Data_Clicks', 'CRMC_PP__Add_Record_Clicks'
  , 'CRMC_PP__Views_Loaded', 'FSTR__Distinct_Users','FSTR__Distinct_Users_30','FSTR__Distinct_Users_90','FSTR__Total_Approval_Steps','FSTR__Total_Checklists'
  , 'FSTR__Total_Checklist_Items','FSTR__Total_Definitions','FSTR__Total_Delayed_Steps','FSTR__Total_Dependencies','FSTR__Total_Document_Generation'
  , 'FSTR__Total_Email_Alerts','FSTR__Total_Event_Placeholders','FSTR__Total_Field_Updates','FSTR__Total_Field_Update_Steps','FSTR__Total_Initiators'
  , 'FSTR__Total_Initiator_Criteria','FSTR__Total_Loop_Back_Steps','FSTR__Total_PCE_Steps','FSTR__Total_Tasks','FSTR__Total_Tasks_90','FSTR__Total_Validations'
  , 'FSTR__Total_Webservice_Callouts','FSTR__User_Count', 'APXT_Redlining__Monthly_Active_Users','APXT_Redlining__Send_for_Negotiation_Events'
  , 'APXT_Redlining__View_Redlines_Page_Events')  
               THEN 1::boolean
        else 0::boolean
       end as C2_DAILY_REPORTING  
     , 0::boolean as C1_ADOPTION_USE               
from APTTUS_DW.SF_CONGA1_1.SFFMA__FEATUREPARAMETER__C

;