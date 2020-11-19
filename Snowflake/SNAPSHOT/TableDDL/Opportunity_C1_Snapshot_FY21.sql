-- APTTUS_DW.SF_PRODUCTION."Opportunity_C1_Snapshot_FY21" definition

--create or replace TABLE "Opportunity_C1_Snapshot_FY21" (
	"Account_Name" VARCHAR(16777216),
	"AccountID" VARCHAR(16777216),
	"OpportunityID" VARCHAR(16777216),
	"Opportunity Name" VARCHAR(16777216),
	"Snapshot Date" DATE,
	"Account Owner Name" VARCHAR(16777216),
	"Segment Territory Name" VARCHAR(16777216),
	"Account Region" VARCHAR(16777216),
	"Region" VARCHAR(16777216),
	"Geo" VARCHAR(16777216),
	"TM Division Name" VARCHAR(16777216),
	"TM Segment Name" VARCHAR(16777216),
	"Segment" VARCHAR(16777216),
	"Division Territory Name" VARCHAR(16777216),
	"CS Division" VARCHAR(16777216),
	"Division Bucket" VARCHAR(16777216),
	"Account.BillingCountry" VARCHAR(16777216),
	"Account.BillingState" VARCHAR(16777216),
	"Account.ShippingCountry" VARCHAR(16777216),
	"Account.ShippingState" VARCHAR(16777216),
	"Account.CreatedDate" TIMESTAMP_TZ(9),
	"Account.Industry" VARCHAR(16777216),
	ARR FLOAT,
	"Average ACV" FLOAT,
	"Fist Year Billings" FLOAT,
	"Fist Year Billings Override" FLOAT,
	"Net New MRR" FLOAT,
	"Net New Discount Recapture ACV" FLOAT,
	"Discount Recapture MRR" FLOAT,
	"TCV Subscriptions" FLOAT,
	"TCV Non Recurring" FLOAT,
	"TCV Services" FLOAT,
	"A1 Aligned Bookings Date" DATE,
	"Bookings Date" DATE,
	"Estimated Close Date" DATE,
	"CreatedDate" DATE,
	"Forecast Category" VARCHAR(16777216),
	"LeadSource" VARCHAR(16777216),
	"Closed Reason Category" VARCHAR(16777216),
	"Closed Reason" VARCHAR(16777216),
	"Type" VARCHAR(16777216),
	"Predicted C2 Type" VARCHAR(16777216),
	"Sub Type" VARCHAR(16777216),
	"StageName" VARCHAR(16777216),
	"Predicted C2 Stage" VARCHAR(16777216),
	"Opportunity Owner" VARCHAR(16777216),
	"Territory Manager" VARCHAR(16777216),
	"Customer Success Manager" VARCHAR(16777216),
	"Booking Owner" VARCHAR(16777216),
	"Sales Engineer" VARCHAR(16777216),
	"Opportunity Source" VARCHAR(16777216),
	"Next Steps" VARCHAR(16777216),
	"Next Step Last Edited" DATE,
	"MRR Sub End" DATE,
	"MRR Sub Start" DATE,
	COVID19RISK BOOLEAN,
	"CS Forecast ARR" FLOAT,
	"CS Forecast Override" FLOAT,
	"CS Forecast ARR Override" VARCHAR(16777216),
	"Current Total ARR" FLOAT,
	"Future Total ARR" FLOAT,
	"Price Increase ARR" FLOAT,
	"Total MRR Churn" FLOAT,
	"Total MRR Downsell" FLOAT,
	"MRR GNMRR" FLOAT,
	"Sales MRR" FLOAT,
	"Product of Interest" VARCHAR(16777216)
);

ALTER TABLE APTTUS_DW.SF_PRODUCTION."Opportunity_C1_Snapshot_FY21" ADD COLUMN "Sales MRR" FLOAT;
ALTER TABLE APTTUS_DW.SF_PRODUCTION."Opportunity_C1_Snapshot_FY21" ADD COLUMN "Product of Interest" VARCHAR(16777216);

/* potential additional column renames
       , "Average ACV"                                          AS "ACV (USD)" -- duplicate of  "Average ACV" 
       , "A1 Aligned Bookings Date"                             AS "Close Date" -- duplicate of "A1 Aligned Bookings Date"
       , "CreatedDate"                                          AS "Created Date" -- duplicate of "CreatedDate"
       , "Predicted C2 Stage"                                   AS "Stage" -- duplicate of "Predicted C2 Stage"    
       , "OpportunityID"                                        AS "Opportunity 18 Digit ID" -- duplicate of  "OpportunityID"
       , "Predicted C2 Type"                                    AS "C2 Type" -- duplicate OF "Predicted C2 Type"  
*/
