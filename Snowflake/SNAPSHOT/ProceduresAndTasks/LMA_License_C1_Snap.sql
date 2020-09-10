show procedures IN SCHEMA SNAPSHOTS;

create or replace procedure APTTUS_DW.SNAPSHOTS.CREATE_LMA_LICENSE_C1_SNAP(FLOAT_PARAM1 FLOAT)
    returns string
    language javascript
    strict
    as
    $$
    var sql_command = "CREATE TABLE APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_SNAP_" + FLOAT_PARAM1 + " as SELECT *, CONVERT_TIMEZONE('UTC',current_timestamp()) AS SNAP_LOAD_AT from APTTUS_DW.SF_CONGA1_0.SFLMA__LICENSE__C"
    try {
        snowflake.execute (
            {sqlText: sql_command}
            );
        return "Succeeded.";   // Return a success/error indicator.
        }
    catch (err)  {
        return "Failed: " + err;   // Return a success/error indicator.
        }
    $$
    ;
    
describe procedure APTTUS_DW.SNAPSHOTS.CREATE_LMA_LICENSE_C1_SNAP(FLOAT);   
--how to call example
CALL APTTUS_DW.SNAPSHOTS.CREATE_LMA_LICENSE_C1_SNAP(select Max(_SDC_TABLE_VERSION) from APTTUS_DW.SF_CONGA1_0.SFLMA__LICENSE__C);

create or replace procedure APTTUS_DW.SNAPSHOTS.UPDATE_LMA_LICENSE_C1_SNAP(FLOAT_PARAM1 FLOAT)
    returns string
    language javascript
    strict
    as
    $$
    var procname = "UPDATE LMA LICENSE C1 SNAP"
    var stepname = "Insert existing"
    var stmt2 = snowflake.createStatement({sqlText: "SELECT START_AFTER_SYSTEMMODSTAMP FROM APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL \
WHERE OUT_CATALOG = 'APTTUS_DW' \
and OUT_SCHEMA = 'SNAPSHOTS' \
and OUT_OBJECT_NAME = 'LMA_LICENSE_C1_SNAP_' \
and OUT_OBJECT_TYPE = 'Snap Table'" 
        });
    var RS = stmt2.execute();
    RS.next();
    var FROMDATE = RS.getColumnValue(1);
    var return_value;
    return_value = "Failed";

    try {
        var stmt = snowflake.createStatement({
            sqlText: "INSERT INTO APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_SNAP_" + FLOAT_PARAM1 + " SELECT *, CONVERT_TIMEZONE('UTC',current_timestamp()) AS SNAP_LOAD_AT from APTTUS_DW.SF_CONGA1_0.SFLMA__LICENSE__C WHERE SYSTEMMODSTAMP > (:1)",       
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
        stepname = "Create Next Snap Iteration" 
        if (errorstr.includes("does not exist")){
            try {
                var stmt3 = snowflake.createStatement({
                sqlText: 'CALL APTTUS_DW.SNAPSHOTS.CREATE_LMA_LICENSE_C1_SNAP(:1)',
                binds: [FLOAT_PARAM1]
                });
    
                var result3 = stmt3.execute();
                return_value = "Created New Snap for LMA_LICENSE_C1 as LMA_LICENSE_C1_SNAP_" + FLOAT_PARAM1;   // Return a success/error indicator.
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
                ,binds: [procname, stepname, err.code, err.state, errorstr2, err.stackTraceTxt]
                });
        }      
    }

    snowflake.execute({
    sqlText:"UPDATE APTTUS_DW.SNAPSHOTS.SNAPSHOT_CONTROL \
   set START_AFTER_SYSTEMMODSTAMP = (SELECT MAX(SYSTEMMODSTAMP) FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_SNAP_" + FLOAT_PARAM1 + ") \
     , LAST_SDC_EXTRACTED_AT = (SELECT MAX(_SDC_EXTRACTED_AT) FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_SNAP_" + FLOAT_PARAM1 + ") \
     , LAST_SNAPSHOT_START = (CONVERT_TIMEZONE('UTC',current_timestamp())) \
     , CURRENT_SDC_TABLE_VERSION = (SELECT MAX(_SDC_TABLE_VERSION) FROM APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_SNAP_" + FLOAT_PARAM1 + ") \
   WHERE OUT_CATALOG = 'APTTUS_DW' \
     and OUT_SCHEMA = 'SNAPSHOTS' \
     and OUT_OBJECT_NAME = 'LMA_LICENSE_C1_SNAP_' \
     and OUT_OBJECT_TYPE = 'Snap Table'"
     }); 
    return return_value;
    $$
    ;     
      
DESCRIBE procedure APTTUS_DW.SNAPSHOTS.UPDATE_LMA_LICENSE_C1_SNAP(FLOAT);
--how to call example
CALL APTTUS_DW.SNAPSHOTS.UPDATE_LMA_LICENSE_C1_SNAP(select Max(_SDC_TABLE_VERSION) from APTTUS_DW.SF_CONGA1_0.SFLMA__LICENSE__C);


CREATE OR REPLACE TASK APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_SNAP
  WAREHOUSE = APTTUS_ADMIN
  SCHEDULE = 'USING CRON 04 16 * * * UTC' 
AS CALL APTTUS_DW.SNAPSHOTS.UPDATE_LMA_LICENSE_C1_SNAP(select Max(_SDC_TABLE_VERSION) from APTTUS_DW.SF_CONGA1_0.SFLMA__LICENSE__C);

DESCRIBE task APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_SNAP;
alter task APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_SNAP suspend; --resume
alter task APTTUS_DW.SNAPSHOTS.LMA_LICENSE_C1_SNAP resume;

show tasks IN SCHEMA SNAPSHOTS;
