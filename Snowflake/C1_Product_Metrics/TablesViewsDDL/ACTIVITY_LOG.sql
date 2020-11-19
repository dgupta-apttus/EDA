CREATE TABLE APTTUS_DW.PRODUCT.ACTIVITY_LOG
       (  procedure_name string
        , step_name string
        , error_code number default 0
        , error_state string default null
        , error_message string default 'Success'
        , stack_trace string default null
        , CREATEDBYID VARCHAR(16777216) default current_user
        , CREATEDTIME TIMESTAMPTZ default CONVERT_TIMEZONE('UTC',current_timestamp())        
        );