USE [CTG_EXPLOITATION]
GO
/****** Object:  StoredProcedure [dbo].[CTG_Check_Replica_Report_Job]    Script Date: 13/04/2022 11:13:29 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

-- =============================================
-- Author:		Ced
-- Create date: 09-02-2022
-- Description:	Add replica check on report job
-- =============================================
--CREATE PROCEDURE [dbo].[CTG_Check_Replica_Report_Job]
ALTER PROCEDURE [dbo].[CTG_Check_Replica_Report_Job]
AS
BEGIN
SET NOCOUNT ON

	BEGIN TRY

-- Declare a variables.

-- Object for lOG ERROR
DECLARE @OBJECT nvarchar(MAX) = 'CTG_Check_Replica_Report_Job'

-- var table to stack job id and job name
DECLARE @TableJob AS TABLE (
		[id_num] [int] IDENTITY(1,1),
		[job_name] [nvarchar](128),
		[job_id] [uniqueidentifier] );


-- Query to get job id from report server with only one step instead of two steps (additionnal step needed to check primary node befor launching)
DECLARE @SQL nvarchar(MAX) = N'USE [msdb]
SELECT sjs.[job_id], sj.[name]
FROM msdb.[dbo].[sysjobs] sj 
 INNER JOIN [msdb].[dbo].[sysjobsteps] sjs ON sj.[job_id] = sjs.[job_id] 
 INNER JOIN [msdb].[dbo].[syscategories] sc ON sj.[category_id] = sc.[category_id]
WHERE sc.[name]=''Report Server''
 AND sj.[name] LIKE ''[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]-[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]-[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]-[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]-[A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9][A-Z0-9]''
GROUP BY sjs.[job_id], sj.[name]
HAVING COUNT(sjs.[job_id]) = 1 ;
'

-- Using INSERT INTO statement to insert data into @v_Table.
INSERT INTO @TableJob ([job_id], [job_name]) 
 EXECUTE sp_executesql @SQL ;

-- SELECT * FROM @TableJob --debug

 IF EXISTS
(
 SELECT * FROM @TableJob
 )
 BEGIN
		PRINT 'JOB WITHOUT CHECK REPLICA STEP EXISTS'


-- Var to insert data in with loop into  @TableSche
DECLARE @MaxIterator INT
DECLARE @Iterator INT

SELECT @MaxIterator = MAX(id_num), @Iterator = 1 FROM @TableJob

--PRINT @MaxIterator --debug
--PRINT @Iterator --debug

-- loop :-)
 WHILE @Iterator <= @MaxIterator
BEGIN 

DECLARE @jobname NVARCHAR(128), @jobid uniqueidentifier
SELECT @jobname = job_name, @jobid = job_id FROM @TableJob where id_num = @Iterator

--PRINT @jobname --debug
--PRINT @jobid  --debug

DECLARE @job nvarchar(MAX)

Set @job = N'USE [msdb]

EXEC sp_add_jobstep
    @job_name = N'''+ @jobname +'''
   ,@step_id = 1
   ,@step_name = N''Check: Primary Replica''
   ,@subsystem = N''TSQL''
   ,@command = N''DECLARE @retval bit = 0;
DECLARE @description sysname;
                
select @description = role_desc
from sys.dm_hadr_availability_replica_cluster_states harcs 
inner join sys.dm_hadr_availability_replica_states hars on hars.replica_id = harcs.replica_id
where harcs.replica_server_name = @@servername and role = 1
        
IF (@description = ''''PRIMARY'''' or (SELECT SERVERPROPERTY (''''IsHadrEnabled'''')) = 0)
	SET @retval = 1;


IF @retval = 0
	RAISERROR(''''Non primary server - exiting.'''', 13, 1); ''

   ,@on_success_action = 3
   ,@on_fail_action = 1 ;

'

--PRINT @job -- debug
  EXECUTE sp_executesql @job ;
	
 SET @Iterator = @Iterator + 1 ;
 END
 END
ELSE
		BEGIN
	    PRINT 'ALL JOB HAVE CHECK REPLICA STEP'

        --Si pas d'erreur on ACK le log dans la table LOG_ERROR
		UPDATE [CTG_EXPLOITATION].[dbo].[LOG_ERROR] SET ACK = 1, ACK_DESC = 'ACK automatique' where OBJECT = @OBJECT AND ACK = 0
		
		END
		
	END TRY
	BEGIN CATCH
		DECLARE @ErrorMessage NVARCHAR(MAX);
		DECLARE @ErrorSeverity INT;
		DECLARE @ErrorState INT;
		DECLARE @ErrorNumber INT;

		SELECT @ErrorMessage = ERROR_MESSAGE(),
			@ErrorSeverity = ERROR_SEVERITY(),
			@ErrorState = ERROR_STATE(),
			@ErrorNumber = ERROR_NUMBER();

		DECLARE @now DATETIME2(0) = GETDATE()

		INSERT INTO [CTG_EXPLOITATION].[dbo].[LOG_ERROR] ([DATABASE], [DATE], [OBJECT], [TAG], [TYPE], [MESSAGE], [SEVERITY], [STATE])
		 VALUES
			   (DB_NAME(), @now, @OBJECT, 'CATCH', 'ERROR', @ErrorMessage, @ErrorSeverity, @ErrorState)

		RAISERROR (@ErrorMessage, -- Message text.
			   @ErrorSeverity, -- Severity.
			   @ErrorState -- State.
			   );
	END CATCH	
			
END