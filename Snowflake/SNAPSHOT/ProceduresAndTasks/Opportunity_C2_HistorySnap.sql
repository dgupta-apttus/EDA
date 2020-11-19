show procedures IN SCHEMA SNAPSHOTS;

--DROP procedure APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORYSNAP(FLOAT);
create or replace procedure APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORYSNAP()
    returns string
    language javascript
    strict
    as
    $$
    var procname = "UPDATE OPPORTUNITY C2 SNAP"
    var stepname = "Insert existing"
    var return_value = "Failed";
    var error_code = 0;

    var insert_command = `
INSERT INTO APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY
SELECT	  CRM_SOURCE
	, OPPORTUNITY_ID
    , current_Date() AS SNAPSHOT_DATE
	, A1_PARTNER
	, ACCOUNT_ID
	, ACCOUNT_NAME
	, ACCOUNT_OWNER_NAME
	, ACCOUNT_URL
	, ANNUAL_RENEWAL
	, ARR
	, AGE_DAYS
	, AGE_MONTHS
	, AVERAGE_ACV
	, BILLING_FREQUENCY
	, BOOKINGS_DATE
	, BOOKING_STAMP
	, C1_PARTNER
	, C2_STAGE
	, C2_TYPE
	, CAMPAIGN_ID
	, CLOSE_BOOKINGS_DATE
	, CLOSED_REASON_CATEGORY
	, CLOSED_REASON_DETAILS
	, CLOSED_REASON_NOTES
	, CLOSED_REASON_SUBCATEGORY_LOSS
	, CREATED_DATE
	, CURRENCY
	, CURRENCY_CONVERSION_RATE
	, CURRENT_AVG_MRR_TOTAL
	, CUSTOMER_SUPPORT_ID
	, CUSTOMER_SUPPORT_NAME
	, DISCOUNT_RECAPTURE_MRR
	, DOWNSELL_CHURN_CATEGORY
	, DOWNSELL_CHURN_SUB_CATEGORY
	, END_DATE
	, ESTIMATED_CLOSE_DATE
	, EXPANSION_DOLLARS
	, FIRST_YEARS_BILLINGS
	, FISCAL_WEEK
	, FORECAST_CATEGORY
	, GEO
	, HIGHEST_ACHIEVED_STAGE
	, INBOUND_OUTBOUND_OPPORTUNITY
	, LAST_EDIT_DATE
	, LEAD_SOURCE
	, LEAD_SOURCE_DETAIL
	, MRR_SUB_END
	, MRR_SUB_START
	, NET_NEW_MRR
	, NEXT_STEP
	, NEXT_STEP_LAST_EDITED
	, NN_DISCOUNT_RECAPTURE_ACV
	, OPPORTUNITY_CHANNEL_SOURCE
	, OPPORTUNITY_NAME
	, OPPORTUNITY_URL
	, OPS_APPROVED
	, OWNER_GEO_STAMP
	, OWNER_ID
	, OWNER_NAME
	, OWNER_ROLE
	, PLATFORM
	, PRIMARY_COMPETITOR
	, PRIMARY_QUOTE_ID
	, PRODUCTS_OF_INTEREST
	, PROBABILITY
	, RAMPED_ACV
	, REGION
	, RENEWED_AMOUNT
	, RENEWAL_DOLLARS
	, RENEWAL_DUE
	, RENEWAL_DUE_DATE
	, RENEWAL_FORECAST_ARR
	, RENEWAL_UPLIFT
	, SALES_ENGINEER_NAME
	, SALES_MRR
	, SALES_OPPORTUNITY_ACCEPTED
	, SALES_OPPORTUNITY_ACCEPTED_DATE
	, SALESFORCE_ORG
	, SE_ID
	, SEGMENT
	, "STAGE"
	, STAGE_1_DATE_OF_ENTRY
	, START_DATE
	, SUB_TYPE
	, TCV_NON_RECURRING
	, TCV_SERVICES
	, TCV_SUBSCRIPTIONS
	, TERM_MONTHS
	, TERRITORY
	, TERRITORY_MANAGER_ID
	, TERRITORY_MANAGER_NAME
	, TM_SEGMENT_NAME
	, TOTAL_DEAL_VALUE
	, TOTAL_RENEWAL_DUE
	, TOTAL_UPSELL_DOWNSELL
	, "TYPE"
	, VALID_UNTIL_DATE
	, X15_DATE
	, XOPPORTUNITY_ID
	, OPPTY_MODSTAMP
	, ACCNT_MODSTAMP
	, LAST_MODSTAMP
    , CONTACT_ID
FROM
	APTTUS_DW.SF_PRODUCTION."Opportunity_C2"
WHERE LAST_MODSTAMP > (:1)
`
    var check_dups = `
select count(*) as DUP_COUNT from (
SELECT COUNT(*) , CRM_SOURCE, OPPORTUNITY_ID, SNAPSHOT_DATE 
FROM APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY
GROUP BY CRM_SOURCE, OPPORTUNITY_ID, SNAPSHOT_DATE 
HAVING COUNT(*) > 1
)
`
    var stmt2 = snowflake.createStatement({sqlText: "SELECT START_AFTER_SYSTEMMODSTAMP FROM APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL \
WHERE OUT_CATALOG = 'APTTUS_DW' \
and OUT_SCHEMA = 'SNAPSHOTS' \
and OUT_OBJECT_NAME = 'OPPORTUNITY_C2_HISTORY' \
and OUT_OBJECT_TYPE = 'Snap Table'" 
        });
    var RS = stmt2.execute();
    RS.next();
    var FROMDATE = RS.getColumnValue(1);

    try {
        snowflake.execute({
            sqlText: insert_command,
            binds: [FROMDATE]
            }); 
        return_value = "Succeeded. With date "+ FROMDATE ;   // Return a success/error indicator.
	    var stmt1 = snowflake.createStatement({sqlText: check_dups});
	    var RS1 = stmt1.execute();
	    RS1.next();
	    var DUP_COUNT = RS1.getColumnValue(1);
        if (DUP_COUNT != 0){
			return_value = "Rows Inserted to OPPORTUNITY_C2_HISTORY but duplicate values found"
            error_code = 1
	    };
        snowflake.execute({
                    sqlText: `insert into APTTUS_DW.SNAPSHOTS.SNAP_ACTIVITY_LOG (procedure_name, step_name, error_code, error_message) VALUES (?,?,?,?)`
                    ,binds: [procname, stepname, error_code, return_value]
                    });   
        }
    catch (err)  {
                var errorstr = err.message.replace(/\n/g, " ")
                return_value = "Failed: " + errorstr + " Code: " + err.code + " State: " + err.state;
                snowflake.execute({
                    sqlText: `insert into APTTUS_DW.SNAPSHOTS.SNAP_ACTIVITY_LOG VALUES (?,?,?,?,?,?,current_user(),CONVERT_TIMEZONE('UTC',current_timestamp()))`
                    ,binds: [procname, stepname, err.code, err.state, errorstr, err.stackTraceTxt]
                    });
        };

    snowflake.execute({
    sqlText:"UPDATE APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL \
   set START_AFTER_SYSTEMMODSTAMP = (SELECT MAX(LAST_MODSTAMP) FROM APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORY) \
     , LAST_SNAPSHOT_START = (CONVERT_TIMEZONE('UTC',current_timestamp())) \
   WHERE OUT_CATALOG = 'APTTUS_DW' \
     and OUT_SCHEMA = 'SNAPSHOTS' \
     and OUT_OBJECT_NAME = 'OPPORTUNITY_C2_HISTORY' \
     and OUT_OBJECT_TYPE = 'Snap Table'"
     }); 
    return return_value;
    $$
    ;     
      
DESCRIBE procedure APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORYSNAP();
--how to call example
CALL APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORYSNAP();

CREATE OR REPLACE TASK APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORYSNAP
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 08 02 * * * America/Los_Angeles'  
AS CALL APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORYSNAP();

DESCRIBE task APTTUS_DW.OPPORTUNITY_C2_HISTORYSNAP;
alter task APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORYSNAP suspend; --resume
alter task APTTUS_DW.SNAPSHOTS.OPPORTUNITY_C2_HISTORYSNAP resume;

show tasks IN SCHEMA SNAPSHOTS;
