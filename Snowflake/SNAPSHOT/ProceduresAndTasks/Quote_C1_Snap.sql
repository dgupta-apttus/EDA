show procedures IN SCHEMA SNAPSHOTS;

create or replace procedure APTTUS_DW.SNAPSHOTS.CREATE_QUOTE_C1_SNAP(FLOAT_PARAM1 FLOAT)
    returns string
    language javascript
    strict
    as
    $$
    var stmt = snowflake.createStatement({sqlText: "SELECT DATE_PART(epoch_millisecond, current_timestamp()) AS EPOCH_SECS"});
    var RS = stmt.execute();
    RS.next();
    var NEW_EPOCH_SECS = RS.getColumnValue(1);
    var TABLE_EPOCH_SECS = FLOAT_PARAM1;
    var return_value;
    return_value = "Failed.";

    var sql_command = "CREATE TABLE APTTUS_DW.SNAPSHOTS.QUOTE_C1_SNAP_" + FLOAT_PARAM1 + "_" + NEW_EPOCH_SECS + " as SELECT *, CONVERT_TIMEZONE('UTC',current_timestamp()) AS SNAP_LOAD_AT from APTTUS_DW.SF_CONGA1_1.SBQQ__QUOTE__C"
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
         return_value = "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        };

    snowflake.execute({
    sqlText:"UPDATE APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL \
   set CURRENT_SNAPSHOT_TABLE_VERSION = '" + NEW_EPOCH_SECS + "' \
, CURRENT_SDC_TABLE_VERSION = '" + TABLE_EPOCH_SECS + "' \
   WHERE OUT_CATALOG = 'APTTUS_DW' \
     and OUT_SCHEMA = 'SNAPSHOTS' \
     and OUT_OBJECT_NAME = 'QUOTE_C1_SNAP_' \
     and OUT_OBJECT_TYPE = 'Snap Table'"
     }); 
    return return_value;
    $$
    ;
    
describe procedure APTTUS_DW.SNAPSHOTS.CREATE_QUOTE_C1_SNAP(FLOAT);   
--how to call example
CALL APTTUS_DW.SNAPSHOTS.CREATE_QUOTE_C1_SNAP(select Max(_SDC_TABLE_VERSION) from APTTUS_DW.SF_CONGA1_1.SBQQ__QUOTE__C);

--DROP procedure APTTUS_DW.SNAPSHOTS.UPDATE_QUOTE_C1_SNAP(FLOAT);
create or replace procedure APTTUS_DW.SNAPSHOTS.UPDATE_QUOTE_C1_SNAP(CHAR_PARAM1 VARCHAR)
    returns string
    language javascript
    strict
    as
    $$
    var procname = "UPDATE QUOTE_C1 SNAP"
    var stepname = "Insert existing"
    var table_codes = CHAR_PARAM1
    var stmt2 = snowflake.createStatement({sqlText: "SELECT START_AFTER_SYSTEMMODSTAMP FROM APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL \
WHERE OUT_CATALOG = 'APTTUS_DW' \
and OUT_SCHEMA = 'SNAPSHOTS' \
and OUT_OBJECT_NAME = 'QUOTE_C1_SNAP_' \
and OUT_OBJECT_TYPE = 'Snap Table'" 
        });
    var RS = stmt2.execute();
    RS.next();
    var FROMDATE = RS.getColumnValue(1);

    var stmt4 = snowflake.createStatement({sqlText: "select Max(_SDC_TABLE_VERSION) as SDC_TABLE_V from APTTUS_DW.SF_CONGA1_1.SBQQ__QUOTE__C"});
    var RS4 = stmt4.execute();
    RS4.next();
    var SDC_TABLE_VERSION = RS4.getColumnValue(1);

    var CURRENT_SDC_TABLE_VERSION = table_codes.substring(0,13)
    if (SDC_TABLE_VERSION != CURRENT_SDC_TABLE_VERSION){
		table_codes = SDC_TABLE_VERSION
    };

    var return_value;
    return_value = "Failed";

    try {
        var stmt = snowflake.createStatement({
            sqlText: "INSERT INTO APTTUS_DW.SNAPSHOTS.QUOTE_C1_SNAP_" + table_codes + " SELECT *, CONVERT_TIMEZONE('UTC',current_timestamp()) AS SNAP_LOAD_AT from APTTUS_DW.SF_CONGA1_1.SBQQ__QUOTE__C WHERE SYSTEMMODSTAMP > (:1)",       
            binds: [FROMDATE]
            });

        var result = stmt.execute();
        return_value = "Succeeded. With date "+ FROMDATE ;   // Return a success/error indicator.
        snowflake.execute({
            sqlText: `insert into APTTUS_DW.SNAPSHOTS.SNAP_ACTIVITY_LOG (procedure_name, step_name) VALUES (?,?)`
            ,binds: [procname, stepname]
            });
        }
    catch (err)  {
        var errorstr = err.message.replace(/\n/g, " ") 
        if (errorstr.includes("does not exist") || errorstr.includes("does not match column list")){
            try {
                stepname = "Create Next Snap Iteration"
                var stmt3 = snowflake.createStatement({
                sqlText: 'CALL APTTUS_DW.SNAPSHOTS.CREATE_QUOTE_C1_SNAP(:1)',
                binds: [SDC_TABLE_VERSION]
                });
    
                var result3 = stmt3.execute();

//update the table name variable when there is a new create
                var stmt5 = snowflake.createStatement({sqlText: "select CURRENT_SDC_TABLE_VERSION || '_' || CURRENT_SNAPSHOT_TABLE_VERSION as NEW_TAB_V \
from APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL \
WHERE OUT_CATALOG = 'APTTUS_DW' \
and OUT_SCHEMA = 'SNAPSHOTS' \
and OUT_OBJECT_NAME = 'QUOTE_C1_SNAP_'" 
                });
                var RS5 = stmt5.execute();
                RS5.next();
                table_codes = RS5.getColumnValue(1);

                return_value = "Created New Snap for QUOTE_C1 as QUOTE_C1_SNAP_" + table_codes;   // Return a success/error indicator.
                snowflake.execute({
                    sqlText: `insert into APTTUS_DW.SNAPSHOTS.SNAP_ACTIVITY_LOG (procedure_name, step_name) VALUES (?,?)`
                    ,binds: [procname, stepname]
                    });
                }
            catch (err) {
                var errorstr2 = err.message.replace(/\n/g, " ")
                return_value = "Failed: " + errorstr2 + " Code: " + err.code + " State: " + err.state;
                snowflake.execute({
                    sqlText: `insert into APTTUS_DW.SNAPSHOTS.SNAP_ACTIVITY_LOG VALUES (?,?,?,?,?,?,current_user(),CONVERT_TIMEZONE('UTC',current_timestamp()))`
                    ,binds: [procname, stepname, err.code, err.state, errorstr2, err.stackTraceTxt]
                    });
            }
        } else {
            return_value = "Failed: " + errorstr + " Code: " + err.code + " State: " + err.state;
            snowflake.execute({
                sqlText: `insert into APTTUS_DW.SNAPSHOTS.SNAP_ACTIVITY_LOG VALUES (?,?,?,?,?,?,current_user(),CONVERT_TIMEZONE('UTC',current_timestamp()))`
                ,binds: [procname, stepname, err.code, err.state, errorstr, err.stackTraceTxt]
                });
        }      
    }

    snowflake.execute({
    sqlText:"UPDATE APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL \
   set START_AFTER_SYSTEMMODSTAMP = (SELECT MAX(SYSTEMMODSTAMP) FROM APTTUS_DW.SNAPSHOTS.QUOTE_C1_SNAP_" + table_codes + ") \
     , LAST_SDC_EXTRACTED_AT = (SELECT MAX(_SDC_EXTRACTED_AT) FROM APTTUS_DW.SNAPSHOTS.QUOTE_C1_SNAP_" + table_codes + ") \
     , LAST_SNAPSHOT_START = (CONVERT_TIMEZONE('UTC',current_timestamp())) \
     , CURRENT_SDC_TABLE_VERSION = (SELECT MAX(_SDC_TABLE_VERSION) FROM APTTUS_DW.SNAPSHOTS.QUOTE_C1_SNAP_" + table_codes + ") \
   WHERE OUT_CATALOG = 'APTTUS_DW' \
     and OUT_SCHEMA = 'SNAPSHOTS' \
     and OUT_OBJECT_NAME = 'QUOTE_C1_SNAP_' \
     and OUT_OBJECT_TYPE = 'Snap Table'"
     }); 
    return return_value;
    $$
    ;     
      
DESCRIBE procedure APTTUS_DW.SNAPSHOTS.UPDATE_QUOTE_C1_SNAP(VARCHAR);
--how to call example
CALL APTTUS_DW.SNAPSHOTS.UPDATE_QUOTE_C1_SNAP(select CURRENT_SDC_TABLE_VERSION || '_' || CURRENT_SNAPSHOT_TABLE_VERSION from APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL WHERE OUT_CATALOG = 'APTTUS_DW' and OUT_SCHEMA = 'SNAPSHOTS' and OUT_OBJECT_NAME = 'QUOTE_C1_SNAP_');



CREATE OR REPLACE TASK APTTUS_DW.SNAPSHOTS.QUOTE_C1_SNAP
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 10 02 * * * America/Los_Angeles'  
AS CALL APTTUS_DW.SNAPSHOTS.UPDATE_QUOTE_C1_SNAP(select CURRENT_SDC_TABLE_VERSION || '_' || CURRENT_SNAPSHOT_TABLE_VERSION from APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL WHERE OUT_CATALOG = 'APTTUS_DW' and OUT_SCHEMA = 'SNAPSHOTS' and OUT_OBJECT_NAME = 'QUOTE_C1_SNAP_');

DESCRIBE task APTTUS_DW.SNAPSHOTS.QUOTE_C1_SNAP;
alter task APTTUS_DW.SNAPSHOTS.QUOTE_C1_SNAP suspend; --resume
alter task APTTUS_DW.SNAPSHOTS.QUOTE_C1_SNAP resume;

show tasks IN SCHEMA SNAPSHOTS;
