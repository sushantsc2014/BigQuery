--create schema SCD_2_Logic;

CREATE OR REPLACE TABLE SCD_2_Logic.Target_Emp_Details(
Emp_Version_Id STRING,
Emp_Number INTEGER,
Record_Start_Time TIMESTAMP,
Record_End_Time TIMESTAMP,
Current_Record_Ind INTEGER,
Delete_Record_Ind INTEGER,
Emp_Name STRING,
Emp_Location STRING,
Emp_Salary INTEGER);




CREATE OR REPLACE TABLE SCD_2_Logic.Stage_Emp_Details(
Emp_Number INTEGER,
Emp_Name STRING,
Emp_Location STRING,
Emp_Salary INTEGER,
File_Dt DATE);


INSERT INTO Stage_Emp_Details VALUES
(102,'James Wade','Texas',36000,'2001-01-01'),(103,'Jack Stewart','LA',39000,'2001-01-01');


DECLARE BEGINTIME TIMESTAMP DEFAUT CAST('1900-01-01' AS TIMESTAMP);
DECLARE ENDTIME TIMESTAMP DFAULT CAST('9999-12-31' AS TIMESTAMP);

CREATE TEMP TABLE Etl_Employee AS
(
SELECT 
SED.Emp_Number,
SED.Emp_Name,
SED.Emp_Location,
SED.Emp_Salary,
SED.File_Dt,
TED.Record_Start_Time AS Original_Record_Start_Time
CASE WHEN TED.Record_Start_Time  IS NULL THEN BEGINTIME
	ELSE CAST(SED.File_Dt AS TIMESTAMP)
END AS Record_Start_Time,
TED.Emp_Version_Id

FROM SCD_2_Logic.Stage_Emp_Details SED
LEFT JOIN 
(SELECT * FROM 
(SELECT RANK() OVER (PARTITION BY Emp_Number ORDER BY Record_Start_Time DESC) emp_rank, * FROM SCD_2_Logic.Target_Emp_Details) WHERE emp_rank=1) TED
ON SED.Emp_Number=TED.Emp_Number
);



MERGE SCD_2_Logic.Target_Emp_Details AS tgt
	USING (
			SELECT 
			src1.Emp_Number AS Join_Key,
			src1.*
			FROM Etl_Employee src1
			
			UNION ALL
			
			SELECT 
			NULL AS Join_Key,
			src2.*
			FROM Etl_Employee src2 WHERE src2.Emp_Version_Id IS NOT NULL
			
		  ) src1
ON src.Join_Key=tgt.Emp_Number AND src.Original_Record_Start_Time=tgt.Record_Start_Time


WHEN NOT MATCHED THEN INSERT
(
Emp_Version_Id,
Emp_Number,
Record_Start_Time,
Record_End_Time,
Current_Record_Ind,
Delete_Record_Ind,
Emp_Name,
Emp_Location,
Emp_Salary
) 
VALUES
(
CAST(MD5(CONCAT(CAST(src.Emp_Number AS STRING),CAST(src.Record_Start_Time AS STRING))) AS STRING),
src.Emp_Number,
src.Record_Start_Time,
ENDTIME,
1,
0,
src.Emp_Name,
src.Emp_Location,
src.Emp_Salary
)


WHEN MATCHED THEN UPDATE
SET 
tgt.Record_End_Time=TIMESTAMP_SUB(CAST(src.File_Dt AS TIMESTAMP), INTERVAL 1 SECOND),
tgt.Current_Record_Ind=0,
tgt.Delete_Record_Ind=1;
			
			
			