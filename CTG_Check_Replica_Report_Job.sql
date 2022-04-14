USE [AdventureWorks2019]
GO

/****** Object:  StoredProcedure [dbo].[CTG_Check_Replica_Report_Job]    Script Date: 03/02/2022 13:39:25 ******/
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

-- var table to stack job schedule
DECLARE @TableSche AS TABLE (
		[id_num] [int] IDENTITY(1,1),
		[job_name] [nvarchar](128),
		[job_id] [uniqueidentifier],
		[schedule_id] [int],
		[schedule_name] [nvarchar](128),
		[enabled] [int],
		[freq_type] [int],
		[freq_interval] [int],
		[freq_subday_type] [int],
		[freq_subday_interval] [int],
		[freq_relative_interval] [int],
		[freq_recurrence_factor] [int],
		[active_start_date] [int],
		[active_end_date] [int],
		[active_start_time] [int],
		[active_end_time] [int],
		[date_created] [datetime],
		[schedule_description] [nvarchar](4000),
		[next_run_date] [int],
		[next_run_time] [int],
		[schedule_uid] [nvarchar](128),
		[job_count] [int]);

-- Query to get job id from report server with only one step instead of two steps : on additionnal to check primary node befor launching
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
	
INSERT INTO @TableSche ([schedule_id],[schedule_name],[enabled],[freq_type],[freq_interval],[freq_subday_type],[freq_subday_interval],[freq_relative_interval],
[freq_recurrence_factor],[active_start_date],[active_end_date],[active_start_time],[active_end_time],[date_created],[schedule_description],[next_run_date],
[next_run_time],[schedule_uid],[job_count])
EXEC msdb.dbo.sp_help_job @job_name = @jobname , @job_aspect = 'SCHEDULES'

UPDATE @TableSche SET [job_name] = @jobname, [job_id] = @jobid  WHERE id_num = @Iterator

 SET @Iterator = @Iterator + 1 ;
 END;

--SELECT * FROM @TableSche --debug

-- Query to create job with an additionnal step to check replica
DECLARE @JOB nvarchar(MAX)


-- Go out from script if no report job if doesn't exist report server job with only one step
-- Main body of script
 IF EXISTS
(
 SELECT * FROM @TableJob
 )
 BEGIN
		PRINT 'JOB WITHOUT CHECK REPLICA STEP EXISTS'
-- Declare a variable:
DECLARE @v_job_id varchar(128);
DECLARE @v_job_name varchar(128);
DECLARE @v_enabled [int];
DECLARE @v_freq_type [int];
DECLARE @v_freq_interval [int];
DECLARE @v_freq_subday_type [int];
DECLARE @v_freq_subday_interval [int];
DECLARE @v_freq_relative_interval [int];
DECLARE @v_freq_recurrence_factor [int];
DECLARE @v_active_start_date [int];
DECLARE @v_active_end_date [int];
DECLARE @v_active_start_time [int];
DECLARE @v_active_end_time [int];
DECLARE @v_schedule_uid [nvarchar](128) ;
-- Declaring a cursor variable to browse @TableSche
DECLARE @My_Cursor CURSOR;

-- Set Select statement for CURSOR variable.
Set @My_Cursor = CURSOR FOR
		SELECT [job_name],
		[job_id],
		[enabled],
		[freq_type],
		[freq_interval],
		[freq_subday_type],
		[freq_subday_interval],
		[freq_relative_interval],
		[freq_recurrence_factor],
		[active_start_date],
		[active_end_date],
		[active_start_time],
		[active_end_time],
		[schedule_uid]
		FROM @TableSche

-- Open Cursor
OPEN @My_Cursor;

-- Move the cursor to the first line.
-- And assign column values to the variables.
FETCH NEXT FROM @My_Cursor INTO @v_job_id, 
@v_job_name,
@v_enabled,
@v_freq_type,
@v_freq_interval,
@v_freq_subday_type,
@v_freq_subday_interval,
@v_freq_relative_interval,
@v_freq_recurrence_factor,
@v_active_start_date,
@v_active_end_date,
@v_active_start_time,
@v_active_end_time,
@v_schedule_uid ;

-- The FETCH statement was successful. ( @@FETCH_STATUS = 0), begin loop
WHILE @@FETCH_STATUS = 0
BEGIN
  PRINT 'job_id = '+ @v_job_id+' / job_name = '+ @v_job_name;

  Set @job = N'USE [msdb]


EXEC msdb.dbo.sp_delete_job @job_id='''+@v_job_id+''', @delete_unused_schedule=1


BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0

IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N''Report Server'' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N''JOB'', @type=N''LOCAL'', @name=N''Report Server''
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name='''+ @v_job_name +''', 
		@enabled=1, 
		@notify_level_eventlog=2, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N''This job is owned by a report server process. Modifying this job could result in database incompatibilities. Use Report Manager or Management Studio to update this job.'', 
		@category_name=N''Report Server'', 
		@owner_login_name=N''ctgsa'', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N''Check: Primary Replica'', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=3, 
		@on_success_step_id=0, 
		@on_fail_action=1, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N''TSQL'', 
		@command=N''DECLARE @retval bit = 0;
DECLARE @description sysname;
                
select @description = role_desc
from sys.dm_hadr_availability_replica_cluster_states harcs 
inner join sys.dm_hadr_availability_replica_states hars on hars.replica_id = harcs.replica_id
where harcs.replica_server_name = @@servername and role = 1
        
IF (@description = ''''PRIMARY'''' or (SELECT SERVERPROPERTY (''''IsHadrEnabled'''')) = 0)
		SET @retval = 1;


IF @retval = 0
		RAISERROR(''''Non primary server - exiting.'''', 13, 1);'', 
		@database_name=N''master'', 
		@flags=0		
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=''' + @v_job_name + '_step_1' + ''', 
		@step_id=2, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N''TSQL'', 
		@command=N''exec [PBI_ReportServer].dbo.AddEvent @EventType=''''DataModelRefresh'''', @EventData=''''73b7ea2e-8655-43be-a1ea-ecf7bd480cd3'''''', 
		@database_name=N''master'', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N''Schedule_1'', 
		@enabled='''+ @v_enabled +''',  
		@freq_type='''+ @v_freq_type +''',  
		@freq_interval='''+ @v_freq_interval +''',  
		@freq_subday_type='''+ @v_freq_subday_type +''', 
		@freq_subday_interval='''+ @v_freq_subday_interval +''',  
		@freq_relative_interval='''+ @v_freq_relative_interval +''',  
		@freq_recurrence_factor='''+ @v_freq_recurrence_factor +''', 
		@active_start_date='''+ @v_active_start_date +''', 
		@active_end_date='''+ @v_active_end_date +''', 
		@active_start_time='''+ @v_active_start_time +''',  
		@active_end_time='''+ @v_active_end_time +''',  
		@schedule_uid='''+ @v_schedule_uid +''', 
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N''(local)''
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

'

-- PRINT @job -- debug
  EXECUTE sp_executesql @JOB ;
  -- Move to the next record.
  -- And assign column values to the variables.
  FETCH NEXT FROM @My_Cursor INTO @v_job_id, @v_job_name;
END

-- Close Cursor.
CLOSE @My_Cursor;
DEALLOCATE @My_Cursor;


 END ;
	ELSE
	    PRINT 'ALL JOB HAVE CHECK REPLICA STEP'

        --Si pas d'erreur on ACK le log dans la table LOG_ERROR
		UPDATE [CTG_EXPLOITATION].[dbo].[LOG_ERROR] SET ACK = 1, ACK_DESC = 'ACK automatique' where OBJECT = @OBJECT AND ACK = 0
		
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

GO
