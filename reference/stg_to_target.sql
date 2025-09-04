

use Kimball_Dev
GO
--AmtInForeignCurrForTaxBreakdown_WRBT1

--======================================
--step01 - BSAK
--======================================
--select * FROM [dbo].[SAP_Naming]  where [Table] = 'BSAK' and sasname = 'AmtInForeignCurrForTaxBreakdown'
--======================================
--Check sasname duplicate
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #duplicate_sasname
SELECT sasname INTO #duplicate_sasname FROM [dbo].[SAP_Naming] 
WHERE [TABLE] = 'BSAK'
GROUP BY sasname HAVING COUNT(*) > 1
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 1'
GO
SELECT * FROM [dbo].[SAP_Naming]
--(4 rows affected) 00:00:00 03/30/25  8:51:37 AM 
--select * from #duplicate_sasname

--select * FROM [dbo].[SAP_Naming] where [TABLE] = 'BSAK' and SasName=  'AmtInForeignCurrForTaxBreakdown'

--======================================
--Get column names from stage table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #bsakname
SELECT [name] stgname INTO #bsakname
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.stg_bsak') --@@ChangeMe if required
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 2'
GO
--select * from #bsakname
--(194 rows affected) 00:00:00 03/30/25  8:51:49 AM 

--======================================
--Get SAP column names
--======================================
-- If duplicate sasname then we are concatenate sasname_field column
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #SAP_Naming
CREATE TABLE #SAP_Naming 
( id int identity(1,1),
[stgname] VARCHAR(100),
[sasname]  VARCHAR(100),
[Datatype]  VARCHAR(100)
)
INSERT INTO #SAP_Naming ([stgname])
SELECT stgname from #bsakname

UPDATE sap SET sap.sasname = CASE WHEN c.[sasname] IS NULL THEN b.[sasname] ELSE b.[sasname] +'_'+b.[field]  END,
sap.Datatype = CASE  WHEN b.[datatype] IN ('NUMC') THEN 'INT' 
WHEN b.[datatype] IN ('CURR') THEN 'DECIMAL('+b.[Length]+',2)' 
WHEN b.[datatype] = 'DATS' THEN 'DATE'  WHEN b.[datatype] IN ('CLNT','CHAR','CUKY') THEN 'VARCHAR('+b.[Length]+')' ELSE 'VARCHAR(100)' END 
from #SAP_Naming sap LEFT JOIN  [dbo].[SAP_Naming]  b ON b.[field] = stgname
LEFT JOIN #duplicate_sasname c ON c.sasname = b.sasname
WHERE b.[TABLE] = 'BSAK' 
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 3'
GO

--(181 rows affected) 00:00:00 03/30/25  8:52:04 AM 

UPDATE #SAP_Naming SET sasname = stgname , Datatype = 'VARCHAR(100)' WHERE sasname IS NULL

UPDATE #SAP_Naming SET Datatype = 'DATETIME' where sasname = 'SAS_IMPORT_DATETIME'

--select * from #SAP_Naming order by 2

--======================================
--Create target table from stg table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DECLARE
@BSAKTargetTable NVARCHAR(256) = 'bsak'

DECLARE @Max_ordinal_position varchar(100) = (SELECT MAX(id) FROM #SAP_Naming)
--select @Max_ordinal_position
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @columnDefinitions VARCHAR(MAX) = ''
-- Get column definitions
SELECT @columnDefinitions += '[' + [SasName] + '] ' + [datatype] + 
CASE WHEN id = @Max_ordinal_position THEN '' ELSE ',' END + CHAR(10)
FROM #SAP_Naming ORDER BY id

--print @columnDefinitions

-- Construct the CREATE TABLE statement
SET @sql = 'CREATE TABLE dbo.' + @BSAKTargetTable + ' (' + CHAR(10) + @columnDefinitions 
 +',[SASREFNBR] [bigint] IDENTITY(1,1) NOT NULL,
 PRIMARY KEY CLUSTERED 
(
	[SASREFNBR] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
);
ALTER TABLE '+@BSAKTargetTable+' ADD  DEFAULT (getdate()) FOR [SAS_IMPORT_DATETIME]
'
--PRINT @sql
exec (@sql);
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 4'
GO
--(0 rows affected) 00:00:00 03/30/25  8:52:23 AM 

--Alter table dbo.stg_bsak 
--alter column AUGDT nvarchar(100)
--Alter table dbo.stg_bsak 
--alter column BUDAT nvarchar(100)
--Alter table dbo.stg_bsak 
--alter column BLDAT nvarchar(100)
--Alter table dbo.stg_bsak 
--alter column CPUDT nvarchar(100)
--Alter table dbo.stg_bsak 
--alter column ZFBDT nvarchar(100)
--Alter table dbo.stg_bsak 
--alter column ZOLLD nvarchar(100)
--Alter table dbo.stg_bsak 
--alter column MADAT nvarchar(100)
--Alter table dbo.stg_bsak 
--alter column DABRZ nvarchar(100)
--Alter table dbo.stg_bsak 
--alter column UEBGDAT nvarchar(100)
--======================================
--Insert stg table data to Target table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
declare @BSAKSourceTablename varchar(max) = 'dbo.stg_bsak'    --- @@ChangeMe if required
declare @BSAKTargetTablename varchar(max) = 'dbo.bsak'   --- @@ChangeMe if required

DECLARE @BSAKqry VARCHAR(MAX) =

+'CREATE OR ALTER PROCEDURE [dbo].[usp_build_'+REPLACE(@BSAKSourceTablename,'dbo.stg_','')+']'+ CHAR(10)   
+'AS  ' + CHAR(10)   
+'BEGIN' + CHAR(10)  
+ CHAR(10)
+'INSERT INTO '+@BSAKTargetTablename +'(' +STUFF((
    SELECT ', ' + char(13) + char(10) + QUOTENAME(c.COLUMN_NAME) 
    FROM INFORMATION_SCHEMA.COLUMNS c  WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @BSAKTargetTablename and  QUOTENAME(c.COLUMN_NAME) NOT LIKE '%SASREFNBR%'
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10)+')'
  + CHAR(10)  
+'SELECT  ' + STUFF((
     SELECT ', ' + char(13) + char(10) +'['+sasname+']'+'	=	' 
	+CASE WHEN datatype = 'INT' THEN +'ISNULL(CAST('+stgname+' AS '+datatype+'),0)'
	  WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 1 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	WHEN datatype LIKE '%DECIMAL%' THEN +'CAST(LEFT('+stgname+',15)' + ' AS ' + datatype + ')'
	ELSE 'CAST(' + stgname + ' AS ' + datatype + ')' END
    FROM #SAP_Naming
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10) + ' FROM ' + @BSAKSourceTablename + ' where sgtxt not like ''%"%'''  --- Add additional where clause if required to filter invalid records
  + CHAR(10) + 'END';
--print @BSAKqry
exec(@BSAKqry)
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 5'
GO
select * from #SAP_Naming
--======================================
--step02 - BSIK
--======================================

--======================================
--Check sasname duplicate
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #duplicate_sasname
SELECT sasname INTO #duplicate_sasname FROM [dbo].[SAP_Naming] 
WHERE [TABLE] = 'BSIK'
GROUP BY sasname HAVING COUNT(*) > 1
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 6'
GO
--select * from #duplicate_sasname
--(4 rows affected) 00:00:00 03/30/25  2:51:18 PM 


--======================================
--Get column names from stage table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #bsikname
SELECT [name] stgname INTO #bsikname
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.stg_bsik') --@@ChangeMe if required
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 7'
GO
--select * from #bsikname
--(194 rows affected) 00:00:00 03/30/25  2:51:37 PM 

--======================================
--Get SAP column names
--======================================
-- If duplicate sasname then we are concatenate sasname_field column
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #SAP_Naming
CREATE TABLE #SAP_Naming 
( id int identity(1,1),
[stgname] VARCHAR(100),
[sasname]  VARCHAR(100),
[Datatype]  VARCHAR(100)
)
INSERT INTO #SAP_Naming ([stgname])
SELECT stgname from #bsikname

UPDATE sap SET sap.sasname = CASE WHEN c.[sasname] IS NULL THEN b.[sasname] ELSE b.[sasname] +'_'+b.[field]  END,
sap.Datatype = CASE  WHEN b.[datatype] IN ('NUMC') THEN 'INT' 
WHEN b.[datatype] IN ('CURR') THEN 'DECIMAL('+b.[Length]+',2)' 
WHEN b.[datatype] = 'DATS' THEN 'DATE'  WHEN b.[datatype] IN ('CLNT','CHAR','CUKY') THEN 'VARCHAR('+b.[Length]+')' ELSE 'VARCHAR(100)' END 
from #SAP_Naming sap LEFT JOIN  [dbo].[SAP_Naming]  b ON b.[field] = stgname
LEFT JOIN #duplicate_sasname c ON c.sasname = b.sasname
WHERE b.[TABLE] = 'BSIK' 

GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 8'
GO

--(181 rows affected) 00:00:00 03/30/25  2:51:57 PM 

UPDATE #SAP_Naming SET sasname = stgname , Datatype = 'VARCHAR(100)' WHERE sasname IS NULL

UPDATE #SAP_Naming SET Datatype = 'DATETIME' where sasname = 'SAS_IMPORT_DATETIME'

--select * from #SAP_Naming order by 2

--drop table dbo.bsik
--======================================
--Create target table from stg table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DECLARE
@BSIKTargetTable NVARCHAR(256) = 'bsik'

DECLARE @Max_ordinal_position varchar(100) = (SELECT MAX(id) FROM #SAP_Naming)
--select @Max_ordinal_position
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @columnDefinitions VARCHAR(MAX) = ''
-- Get column definitions
SELECT @columnDefinitions += '[' + [SasName] + '] ' + [datatype] + 
CASE WHEN id = @Max_ordinal_position THEN '' ELSE ',' END + CHAR(10)
FROM #SAP_Naming ORDER BY id

--print @columnDefinitions

-- Construct the CREATE TABLE statement
SET @sql = 'CREATE TABLE dbo.' + @BSIKTargetTable + ' (' + CHAR(10) + @columnDefinitions 
 +',[SASREFNBR] [bigint] IDENTITY(1,1) NOT NULL,
 PRIMARY KEY CLUSTERED 
(
	[SASREFNBR] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
);
ALTER TABLE '+@BSIKTargetTable+' ADD  DEFAULT (getdate()) FOR [SAS_IMPORT_DATETIME]
'
--PRINT @sql
exec (@sql);
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 9'
GO

--(0 rows affected) 00:00:00 03/30/25  2:52:21 PM 
--======================================
--Insert stg table data to Target table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
declare @BSIKSourceTablename varchar(max) = 'dbo.stg_bsik'    --- @@ChangeMe if required
declare @BSIKTargetTablename varchar(max) = 'dbo.bsik'   --- @@ChangeMe if required

DECLARE @BSIKqry VARCHAR(MAX) =

+'CREATE OR ALTER PROCEDURE [dbo].[usp_build_'+REPLACE(@BSIKSourceTablename,'dbo.stg_','')+']'+ CHAR(10)   
+'AS  ' + CHAR(10)   
+'BEGIN' + CHAR(10)  
+ CHAR(10)
+'INSERT INTO '+@BSIKTargetTablename +'(' +STUFF((
    SELECT ', ' + char(13) + char(10) + QUOTENAME(c.COLUMN_NAME) 
    FROM INFORMATION_SCHEMA.COLUMNS c  WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @BSIKTargetTablename and  QUOTENAME(c.COLUMN_NAME) NOT LIKE '%SASREFNBR%'
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10)+')'
  + CHAR(10)  
+'SELECT  ' + STUFF((
     SELECT ', ' + char(13) + char(10) +'['+sasname+']'+'	=	' 
	+CASE WHEN datatype = 'INT' THEN +'ISNULL(CAST('+stgname+' AS '+datatype+'),0)'
	  WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 1 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	 -- WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 0 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	WHEN datatype LIKE '%DECIMAL%' THEN +'CAST(LEFT('+stgname+',15)' + ' AS ' + datatype + ')'
	ELSE 'CAST(' + stgname + ' AS ' + datatype + ')' END
    FROM #SAP_Naming
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10) + ' FROM ' + @BSIKSourceTablename   --- Add additional where clause if required to filter invalid records
  + CHAR(10) + 'END';
--print @BSIKqry
exec(@BSIKqry)
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 10'
GO

select * from #SAP_Naming
--======================================
--step03 - LFA1
--======================================

--======================================
--Check sasname duplicate
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #duplicate_sasname
SELECT sasname INTO #duplicate_sasname FROM [dbo].[SAP_Naming] 
WHERE [TABLE] = 'LFA1'
GROUP BY sasname HAVING COUNT(*) > 1
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 11'
GO
--select * from #duplicate_sasname
--(5 rows affected) 00:00:00 03/30/25  3:31:02 PM 
--======================================
--Get column names from stage table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #lfa1name
SELECT [name] stgname INTO #lfa1name
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.stg_lfa1') --@@ChangeMe if required
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 12'
GO
--select * from #lfa1name
--(160 rows affected) 00:00:00 03/30/25  3:31:22 PM 
--======================================
--Get SAP column names
--======================================
-- If duplicate sasname then we are concatenate sasname_field column
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #SAP_Naming
CREATE TABLE #SAP_Naming 
( id int identity(1,1),
[stgname] VARCHAR(100),
[sasname]  VARCHAR(100),
[Datatype]  VARCHAR(100)
)
INSERT INTO #SAP_Naming ([stgname])
SELECT stgname from #lfa1name

UPDATE sap SET sap.sasname = CASE WHEN c.[sasname] IS NULL THEN b.[sasname] ELSE b.[sasname] +'_'+b.[field]  END,
sap.Datatype = CASE  WHEN b.[datatype] IN ('NUMC') THEN 'INT' 
WHEN b.[datatype] IN ('CURR') THEN 'DECIMAL('+b.[Length]+',2)' 
WHEN b.[datatype] = 'DATS' THEN 'DATE'  WHEN b.[datatype] IN ('CLNT','CHAR','CUKY') THEN 'VARCHAR('+b.[Length]+')' ELSE 'VARCHAR(100)' END 
from #SAP_Naming sap LEFT JOIN  [dbo].[SAP_Naming]  b ON b.[field] = stgname
LEFT JOIN #duplicate_sasname c ON c.sasname = b.sasname
WHERE b.[TABLE] = 'LFA1' 

GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 13'
GO

--(137 rows affected) 00:00:00 03/30/25  3:31:49 PM 

UPDATE #SAP_Naming SET sasname = stgname , Datatype = 'VARCHAR(100)' WHERE sasname IS NULL

UPDATE #SAP_Naming SET Datatype = 'DATETIME' where sasname = 'SAS_IMPORT_DATETIME'

--select * from #SAP_Naming order by 2

--======================================
--Create target table from stg table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DECLARE
@LFA1TargetTable NVARCHAR(256) = 'lfa1'

DECLARE @Max_ordinal_position varchar(100) = (SELECT MAX(id) FROM #SAP_Naming)
--select @Max_ordinal_position
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @columnDefinitions VARCHAR(MAX) = ''
-- Get column definitions
SELECT @columnDefinitions += '[' + [SasName] + '] ' + [datatype] + 
CASE WHEN id = @Max_ordinal_position THEN '' ELSE ',' END + CHAR(10)
FROM #SAP_Naming ORDER BY id

--print @columnDefinitions

-- Construct the CREATE TABLE statement
SET @sql = 'CREATE TABLE dbo.' + @LFA1TargetTable + ' (' + CHAR(10) + @columnDefinitions 
 +',[SASREFNBR] [bigint] IDENTITY(1,1) NOT NULL,
 PRIMARY KEY CLUSTERED 
(
	[SASREFNBR] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
);
ALTER TABLE '+@LFA1TargetTable+' ADD  DEFAULT (getdate()) FOR [SAS_IMPORT_DATETIME]
'
--PRINT @sql
exec (@sql);
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 14'
GO
--(0 rows affected) 00:00:00 03/30/25  3:32:13 PM 

--======================================
--Insert stg table data to Target table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
declare @LFA1SourceTablename varchar(max) = 'dbo.stg_lfa1'    --- @@ChangeMe if required
declare @LFA1TargetTablename varchar(max) = 'dbo.lfa1'   --- @@ChangeMe if required

DECLARE @LFA1qry VARCHAR(MAX) =

+'CREATE OR ALTER PROCEDURE [dbo].[usp_build_'+REPLACE(@LFA1SourceTablename,'dbo.stg_','')+']'+ CHAR(10)   
+'AS  ' + CHAR(10)   
+'BEGIN' + CHAR(10)  
+ CHAR(10)
+'INSERT INTO '+@LFA1TargetTablename +'(' +STUFF((
    SELECT ', ' + char(13) + char(10) + QUOTENAME(c.COLUMN_NAME) 
    FROM INFORMATION_SCHEMA.COLUMNS c  WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @LFA1TargetTablename and  QUOTENAME(c.COLUMN_NAME) NOT LIKE '%SASREFNBR%'
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10)+')'
  + CHAR(10)  
+'SELECT  ' + STUFF((
     SELECT ', ' + char(13) + char(10) +'['+sasname+']'+'	=	' 
	+CASE WHEN datatype = 'INT' THEN +'ISNULL(CAST('+stgname+' AS '+datatype+'),0)'
	  WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 1 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	 -- WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 0 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	WHEN datatype LIKE '%DECIMAL%' THEN +'CAST(LEFT('+stgname+',15)' + ' AS ' + datatype + ')'
	ELSE 'CAST(' + stgname + ' AS ' + datatype + ')' END
    FROM #SAP_Naming
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10) + ' FROM ' + @LFA1SourceTablename   --- Add additional where clause if required to filter invalid records
  + CHAR(10) + 'END';
--print @LFA1qry
exec(@LFA1qry)
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 15'
GO


--======================================
--step04 - LFB1
--======================================

--======================================
--Check sasname duplicate
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #duplicate_sasname
SELECT sasname INTO #duplicate_sasname FROM [dbo].[SAP_Naming] 
WHERE [TABLE] = 'LFB1'
GROUP BY sasname HAVING COUNT(*) > 1
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 16'
GO
--select * from #duplicate_sasname

--======================================
--Get column names from stage table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #lfb1name
SELECT [name] stgname INTO #lfb1name
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.stg_lfb1') --@@ChangeMe if required
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 17'
GO

--(85 rows affected) 00:00:00 03/30/25  3:50:07 PM 
--select * from #lfb1name

--======================================
--Get SAP column names
--======================================
-- If duplicate sasname then we are concatenate sasname_field column
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #SAP_Naming
CREATE TABLE #SAP_Naming 
( id int identity(1,1),
[stgname] VARCHAR(100),
[sasname]  VARCHAR(100),
[Datatype]  VARCHAR(100)
)
INSERT INTO #SAP_Naming ([stgname])
SELECT stgname from #lfb1name

UPDATE sap SET sap.sasname = CASE WHEN c.[sasname] IS NULL THEN b.[sasname] ELSE b.[sasname] +'_'+b.[field]  END,
sap.Datatype = CASE  WHEN b.[datatype] IN ('NUMC') THEN 'INT' 
WHEN b.[datatype] IN ('CURR') THEN 'DECIMAL('+b.[Length]+',2)' 
WHEN b.[datatype] = 'DATS' THEN 'DATE'  WHEN b.[datatype] IN ('CLNT','CHAR','CUKY') THEN 'VARCHAR('+b.[Length]+')' ELSE 'VARCHAR(100)' END 
from #SAP_Naming sap LEFT JOIN  [dbo].[SAP_Naming]  b ON b.[field] = stgname
LEFT JOIN #duplicate_sasname c ON c.sasname = b.sasname
WHERE b.[TABLE] = 'LFB1' 

GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 18'
GO

--(71 rows affected) 00:00:00 03/30/25  3:50:26 PM 

UPDATE #SAP_Naming SET sasname = stgname , Datatype = 'VARCHAR(100)' WHERE sasname IS NULL

UPDATE #SAP_Naming SET Datatype = 'DATETIME' where sasname = 'SAS_IMPORT_DATETIME'

--select * from #SAP_Naming order by 2

--======================================
--Create target table from stg table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DECLARE
@LFB1TargetTable NVARCHAR(256) = 'lfb1'

DECLARE @Max_ordinal_position varchar(100) = (SELECT MAX(id) FROM #SAP_Naming)
--select @Max_ordinal_position
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @columnDefinitions VARCHAR(MAX) = ''
-- Get column definitions
SELECT @columnDefinitions += '[' + [SasName] + '] ' + [datatype] + 
CASE WHEN id = @Max_ordinal_position THEN '' ELSE ',' END + CHAR(10)
FROM #SAP_Naming ORDER BY id

--print @columnDefinitions

-- Construct the CREATE TABLE statement
SET @sql = 'CREATE TABLE dbo.' + @LFB1TargetTable + ' (' + CHAR(10) + @columnDefinitions 
 +',[SASREFNBR] [bigint] IDENTITY(1,1) NOT NULL,
 PRIMARY KEY CLUSTERED 
(
	[SASREFNBR] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
);
ALTER TABLE '+@LFB1TargetTable+' ADD  DEFAULT (getdate()) FOR [SAS_IMPORT_DATETIME]
'
--PRINT @sql
exec (@sql);
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 19'
GO

--(0 rows affected) 00:00:00 03/30/25  3:50:51 PM 

--======================================
--Insert stg table data to Target table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
declare @LFB1SourceTablename varchar(max) = 'dbo.stg_lfb1'    --- @@ChangeMe if required
declare @LFB1TargetTablename varchar(max) = 'dbo.lfb1'   --- @@ChangeMe if required

DECLARE @LFB1qry VARCHAR(MAX) =

+'CREATE OR ALTER PROCEDURE [dbo].[usp_build_'+REPLACE(@LFB1SourceTablename,'dbo.stg_','')+']'+ CHAR(10)   
+'AS  ' + CHAR(10)   
+'BEGIN' + CHAR(10)  
+ CHAR(10)
+'INSERT INTO '+@LFB1TargetTablename +'(' +STUFF((
    SELECT ', ' + char(13) + char(10) + QUOTENAME(c.COLUMN_NAME) 
    FROM INFORMATION_SCHEMA.COLUMNS c  WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @LFB1TargetTablename and  QUOTENAME(c.COLUMN_NAME) NOT LIKE '%SASREFNBR%'
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10)+')'
  + CHAR(10)  
+'SELECT  ' + STUFF((
     SELECT ', ' + char(13) + char(10) +'['+sasname+']'+'	=	' 
	+CASE WHEN datatype = 'INT' THEN +'ISNULL(CAST('+stgname+' AS '+datatype+'),0)'
	  WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 1 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	 WHEN datatype LIKE '%DECIMAL%' THEN +'CAST(LEFT('+stgname+',15)' + ' AS ' + datatype + ')'
	ELSE 'CAST(' + stgname + ' AS ' + datatype + ')' END
    FROM #SAP_Naming
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10) + ' FROM ' + @LFB1SourceTablename   --- Add additional where clause if required to filter invalid records
  + CHAR(10) + 'END';
--print @LFB1qry
exec(@LFB1qry)
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 20'
GO
--(0 rows affected) 00:00:00 03/30/25  3:51:35 PM 

--======================================
--step05 - LFM1
--======================================

--======================================
--Check sasname duplicate
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #duplicate_sasname
SELECT sasname INTO #duplicate_sasname FROM [dbo].[SAP_Naming] 
WHERE [TABLE] = 'LFM1'
GROUP BY sasname HAVING COUNT(*) > 1
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 21'
GO
--select * from #duplicate_sasname
--(0 rows affected) 00:00:00 03/30/25  3:51:48 PM 
--======================================
--Get column names from stage table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #lfm1name
SELECT [name] stgname INTO #lfm1name
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.stg_lfm1') --@@ChangeMe if required
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 22'
GO
--select * from #lfm1name
--(68 rows affected) 00:00:00 03/30/25  3:52:09 PM 
--======================================
--Get SAP column names
--======================================
-- If duplicate sasname then we are concatenate sasname_field column
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #SAP_Naming
CREATE TABLE #SAP_Naming 
( id int identity(1,1),
[stgname] VARCHAR(100),
[sasname]  VARCHAR(100),
[Datatype]  VARCHAR(100)
)
INSERT INTO #SAP_Naming ([stgname])
SELECT stgname from #lfm1name

UPDATE sap SET sap.sasname = CASE WHEN c.[sasname] IS NULL THEN b.[sasname] ELSE b.[sasname] +'_'+b.[field]  END,
sap.Datatype = CASE  WHEN b.[datatype] IN ('NUMC') THEN 'INT' 
WHEN b.[datatype] IN ('CURR') THEN 'DECIMAL('+b.[Length]+',2)' 
WHEN b.[datatype] = 'DATS' THEN 'DATE'  WHEN b.[datatype] IN ('CLNT','CHAR','CUKY') THEN 'VARCHAR('+b.[Length]+')' ELSE 'VARCHAR(100)' END 
from #SAP_Naming sap LEFT JOIN  [dbo].[SAP_Naming]  b ON b.[field] = stgname
LEFT JOIN #duplicate_sasname c ON c.sasname = b.sasname
WHERE b.[TABLE] = 'LFM1' 

GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 23'
GO

--(55 rows affected) 00:00:00 03/30/25  3:52:26 PM 

UPDATE #SAP_Naming SET sasname = stgname , Datatype = 'VARCHAR(100)' WHERE sasname IS NULL

UPDATE #SAP_Naming SET Datatype = 'DATETIME' where sasname = 'SAS_IMPORT_DATETIME'

--select * from #SAP_Naming order by 2

--======================================
--Create target table from stg table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DECLARE
@LFM1TargetTable NVARCHAR(256) = 'lfm1'

DECLARE @Max_ordinal_position varchar(100) = (SELECT MAX(id) FROM #SAP_Naming)
--select @Max_ordinal_position
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @columnDefinitions VARCHAR(MAX) = ''
-- Get column definitions
SELECT @columnDefinitions += '[' + [SasName] + '] ' + [datatype] + 
CASE WHEN id = @Max_ordinal_position THEN '' ELSE ',' END + CHAR(10)
FROM #SAP_Naming ORDER BY id

--print @columnDefinitions

-- Construct the CREATE TABLE statement
SET @sql = 'CREATE TABLE dbo.' + @LFM1TargetTable + ' (' + CHAR(10) + @columnDefinitions 
 +',[SASREFNBR] [bigint] IDENTITY(1,1) NOT NULL,
 PRIMARY KEY CLUSTERED 
(
	[SASREFNBR] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
);
ALTER TABLE '+@LFM1TargetTable+' ADD  DEFAULT (getdate()) FOR [SAS_IMPORT_DATETIME]
'
--PRINT @sql
exec (@sql);
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 24'
GO
--(0 rows affected) 00:00:00 03/30/25  3:52:47 PM 
--======================================
--Insert stg table data to Target table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
declare @LFM1SourceTablename varchar(max) = 'dbo.stg_lfm1'    --- @@ChangeMe if required
declare @LFM1TargetTablename varchar(max) = 'dbo.lfm1'   --- @@ChangeMe if required

DECLARE @LFM1qry VARCHAR(MAX) =

+'CREATE OR ALTER PROCEDURE [dbo].[usp_build_'+REPLACE(@LFM1SourceTablename,'dbo.stg_','')+']'+ CHAR(10)   
+'AS  ' + CHAR(10)   
+'BEGIN' + CHAR(10)  
+ CHAR(10)
+'INSERT INTO '+@LFM1TargetTablename +'(' +STUFF((
    SELECT ', ' + char(13) + char(10) + QUOTENAME(c.COLUMN_NAME) 
    FROM INFORMATION_SCHEMA.COLUMNS c  WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @LFM1TargetTablename and  QUOTENAME(c.COLUMN_NAME) NOT LIKE '%SASREFNBR%'
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10)+')'
  + CHAR(10)  
+'SELECT  ' + STUFF((
     SELECT ', ' + char(13) + char(10) +'['+sasname+']'+'	=	' 
	+CASE WHEN datatype = 'INT' THEN +'ISNULL(CAST('+stgname+' AS '+datatype+'),0)'
	  WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 1 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	 WHEN datatype LIKE '%DECIMAL%' THEN +'CAST(LEFT('+stgname+',15)' + ' AS ' + datatype + ')'
	ELSE 'CAST(' + stgname + ' AS ' + datatype + ')' END
    FROM #SAP_Naming
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10) + ' FROM ' + @LFM1SourceTablename   --- Add additional where clause if required to filter invalid records
  + CHAR(10) + 'END';
--print @LFM1qry
exec(@LFM1qry)
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 25'
GO
--(0 rows affected) 00:00:00 03/30/25  3:54:06 PM 

--======================================
--step06 - PAYR
--======================================

--======================================
--Check sasname duplicate
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #duplicate_sasname
SELECT sasname INTO #duplicate_sasname FROM [dbo].[SAP_Naming] 
WHERE [TABLE] = 'PAYR'
GROUP BY sasname HAVING COUNT(*) > 1
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 26'
GO
--select * from #duplicate_sasname
--(2 rows affected) 00:00:00 03/30/25  3:54:22 PM 

--======================================
--Get column names from stage table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #payrname
SELECT [name] stgname INTO #payrname
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.stg_payr') --@@ChangeMe if required
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 27'
GO
--select * from #payrname
--(76 rows affected) 00:00:00 03/30/25  3:54:42 PM 
--======================================
--Get SAP column names
--======================================
-- If duplicate sasname then we are concatenate sasname_field column
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #SAP_Naming
CREATE TABLE #SAP_Naming 
( id int identity(1,1),
[stgname] VARCHAR(100),
[sasname]  VARCHAR(100),
[Datatype]  VARCHAR(100)
)
INSERT INTO #SAP_Naming ([stgname])
SELECT stgname from #payrname

UPDATE sap SET sap.sasname = CASE WHEN c.[sasname] IS NULL THEN b.[sasname] ELSE b.[sasname] +'_'+b.[field]  END,
sap.Datatype = CASE  WHEN b.[datatype] IN ('NUMC') THEN 'INT' 
WHEN b.[datatype] IN ('CURR') THEN 'DECIMAL('+b.[Length]+',2)' 
WHEN b.[datatype] = 'DATS' THEN 'DATE'  WHEN b.[datatype] IN ('CLNT','CHAR','CUKY') THEN 'VARCHAR('+b.[Length]+')' ELSE 'VARCHAR(100)' END 
from #SAP_Naming sap LEFT JOIN  [dbo].[SAP_Naming]  b ON b.[field] = stgname
LEFT JOIN #duplicate_sasname c ON c.sasname = b.sasname
WHERE b.[TABLE] = 'PAYR' 

GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 28'
GO

--(63 rows affected) 00:00:00 03/30/25  3:55:00 PM 

UPDATE #SAP_Naming SET sasname = stgname , Datatype = 'VARCHAR(100)' WHERE sasname IS NULL

UPDATE #SAP_Naming SET Datatype = 'DATETIME' where sasname = 'SAS_IMPORT_DATETIME'

--select * from #SAP_Naming order by 2

--======================================
--Create target table from stg table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DECLARE
@PAYRTargetTable NVARCHAR(256) = 'payr'

DECLARE @Max_ordinal_position varchar(100) = (SELECT MAX(id) FROM #SAP_Naming)
--select @Max_ordinal_position
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @columnDefinitions VARCHAR(MAX) = ''
-- Get column definitions
SELECT @columnDefinitions += '[' + [SasName] + '] ' + [datatype] + 
CASE WHEN id = @Max_ordinal_position THEN '' ELSE ',' END + CHAR(10)
FROM #SAP_Naming ORDER BY id

--print @columnDefinitions

-- Construct the CREATE TABLE statement
SET @sql = 'CREATE TABLE dbo.' + @PAYRTargetTable + ' (' + CHAR(10) + @columnDefinitions 
 +',[SASREFNBR] [bigint] IDENTITY(1,1) NOT NULL,
 PRIMARY KEY CLUSTERED 
(
	[SASREFNBR] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
);
ALTER TABLE '+@PAYRTargetTable+' ADD  DEFAULT (getdate()) FOR [SAS_IMPORT_DATETIME]
'
--PRINT @sql
exec (@sql);
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 29'
GO
--(0 rows affected) 00:00:00 03/30/25  3:55:24 PM 
--======================================
--Insert stg table data to Target table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
declare @PAYRSourceTablename varchar(max) = 'dbo.stg_payr'    --- @@ChangeMe if required
declare @PAYRTargetTablename varchar(max) = 'dbo.payr'   --- @@ChangeMe if required

DECLARE @PAYRqry VARCHAR(MAX) =

+'CREATE OR ALTER PROCEDURE [dbo].[usp_build_'+REPLACE(@PAYRSourceTablename,'dbo.stg_','')+']'+ CHAR(10)   
+'AS  ' + CHAR(10)   
+'BEGIN' + CHAR(10)  
+ CHAR(10)
+'INSERT INTO '+@PAYRTargetTablename +'(' +STUFF((
    SELECT ', ' + char(13) + char(10) + QUOTENAME(c.COLUMN_NAME) 
    FROM INFORMATION_SCHEMA.COLUMNS c  WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @PAYRTargetTablename and  QUOTENAME(c.COLUMN_NAME) NOT LIKE '%SASREFNBR%'
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10)+')'
  + CHAR(10)  
+'SELECT  ' + STUFF((
     SELECT ', ' + char(13) + char(10) +'['+sasname+']'+'	=	' 
	+CASE WHEN datatype = 'INT' THEN +'ISNULL(CAST('+stgname+' AS '+datatype+'),0)'
	  WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 1 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	 WHEN datatype LIKE '%DECIMAL%' THEN +'CAST(LEFT('+stgname+',15)' + ' AS ' + datatype + ')'
	ELSE 'CAST(' + stgname + ' AS ' + datatype + ')' END
    FROM #SAP_Naming
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10) + ' FROM ' + @PAYRSourceTablename   --- Add additional where clause if required to filter invalid records
  + CHAR(10) + 'END';
--print @PAYRqry
exec(@PAYRqry)
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 30'
GO
--(0 rows affected) 00:00:00 03/30/25  3:56:03 PM 

--======================================
--step07 - REGUH
--======================================

--======================================
--Check sasname duplicate
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #duplicate_sasname
SELECT sasname INTO #duplicate_sasname FROM [dbo].[SAP_Naming] 
WHERE [TABLE] = 'REGUH'
GROUP BY sasname HAVING COUNT(*) > 1
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 31'
GO
--select * from #duplicate_sasname
--(13 rows affected) 00:00:00 03/30/25  3:56:14 PM 
--======================================
--Get column names from stage table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #reguhname
SELECT [name] stgname INTO #reguhname
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.stg_reguh') --@@ChangeMe if required
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 32'
GO
--select * from #reguhname
--(189 rows affected) 00:00:00 03/30/25  3:56:32 PM 
--======================================
--Get SAP column names
--======================================
-- If duplicate sasname then we are concatenate sasname_field column
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #SAP_Naming
CREATE TABLE #SAP_Naming 
( id int identity(1,1),
[stgname] VARCHAR(100),
[sasname]  VARCHAR(100),
[Datatype]  VARCHAR(100)
)
INSERT INTO #SAP_Naming ([stgname])
SELECT stgname from #reguhname

UPDATE sap SET sap.sasname = CASE WHEN c.[sasname] IS NULL THEN b.[sasname] ELSE b.[sasname] +'_'+b.[field]  END,
sap.Datatype = CASE  WHEN b.[datatype] IN ('NUMC') THEN 'INT' 
WHEN b.[datatype] IN ('CURR') THEN 'DECIMAL('+b.[Length]+',2)' 
WHEN b.[datatype] = 'DATS' THEN 'DATE'  WHEN b.[datatype] IN ('CLNT','CHAR','CUKY') THEN 'VARCHAR('+b.[Length]+')' ELSE 'VARCHAR(100)' END 
from #SAP_Naming sap LEFT JOIN  [dbo].[SAP_Naming]  b ON b.[field] = stgname
LEFT JOIN #duplicate_sasname c ON c.sasname = b.sasname
WHERE b.[TABLE] = 'REGUH' 

GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 33'
GO

--(176 rows affected) 00:00:00 03/30/25  3:56:43 PM 

UPDATE #SAP_Naming SET sasname = stgname , Datatype = 'VARCHAR(100)' WHERE sasname IS NULL

UPDATE #SAP_Naming SET Datatype = 'DATETIME' where sasname = 'SAS_IMPORT_DATETIME'

--select * from #SAP_Naming order by 2

--======================================
--Create target table from stg table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DECLARE
@REGUHTargetTable NVARCHAR(256) = 'reguh'

DECLARE @Max_ordinal_position varchar(100) = (SELECT MAX(id) FROM #SAP_Naming)
--select @Max_ordinal_position
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @columnDefinitions VARCHAR(MAX) = ''
-- Get column definitions
SELECT @columnDefinitions += '[' + [SasName] + '] ' + [datatype] + 
CASE WHEN id = @Max_ordinal_position THEN '' ELSE ',' END + CHAR(10)
FROM #SAP_Naming ORDER BY id

--print @columnDefinitions

-- Construct the CREATE TABLE statement
SET @sql = 'CREATE TABLE dbo.' + @REGUHTargetTable + ' (' + CHAR(10) + @columnDefinitions 
 +',[SASREFNBR] [bigint] IDENTITY(1,1) NOT NULL,
 PRIMARY KEY CLUSTERED 
(
	[SASREFNBR] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
);
ALTER TABLE '+@REGUHTargetTable+' ADD  DEFAULT (getdate()) FOR [SAS_IMPORT_DATETIME]
'
--PRINT @sql
exec (@sql);
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 34'
GO

--======================================
--Insert stg table data to Target table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
declare @REGUHSourceTablename varchar(max) = 'dbo.stg_reguh'    --- @@ChangeMe if required
declare @REGUHTargetTablename varchar(max) = 'dbo.reguh'   --- @@ChangeMe if required

DECLARE @REGUHqry VARCHAR(MAX) =

+'CREATE OR ALTER PROCEDURE [dbo].[usp_build_'+REPLACE(@REGUHSourceTablename,'dbo.stg_','')+']'+ CHAR(10)   
+'AS  ' + CHAR(10)   
+'BEGIN' + CHAR(10)  
+ CHAR(10)
+'INSERT INTO '+@REGUHTargetTablename +'(' +STUFF((
    SELECT ', ' + char(13) + char(10) + QUOTENAME(c.COLUMN_NAME) 
    FROM INFORMATION_SCHEMA.COLUMNS c  WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @REGUHTargetTablename and  QUOTENAME(c.COLUMN_NAME) NOT LIKE '%SASREFNBR%'
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10)+')'
  + CHAR(10)  
+'SELECT  ' + STUFF((
     SELECT ', ' + char(13) + char(10) +'['+sasname+']'+'	=	' 
	+CASE WHEN datatype = 'INT' THEN +'ISNULL(CAST('+stgname+' AS '+datatype+'),0)'
	  WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 1 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	 WHEN datatype LIKE '%DECIMAL%' THEN +'CAST(LEFT('+stgname+',15)' + ' AS ' + datatype + ')'
	ELSE 'CAST(' + stgname + ' AS ' + datatype + ')' END
    FROM #SAP_Naming
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10) + ' FROM ' + @REGUHSourceTablename   --- Add additional where clause if required to filter invalid records
  + CHAR(10) + 'END';
--print @REGUHqry
exec(@REGUHqry)
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 35'
GO

--(0 rows affected) 00:00:00 03/30/25  3:57:37 PM 







--======================================
--step08 - bvor
--======================================
--select * FROM [dbo].[SAP_Naming]  where [Table] = 'BSAK' and sasname = 'AmtInForeignCurrForTaxBreakdown'
--======================================
--Check sasname duplicate
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #duplicate_sasname
SELECT sasname INTO #duplicate_sasname FROM [dbo].[SAP_Naming] 
WHERE [TABLE] = 'BVOR'
GROUP BY sasname HAVING COUNT(*) > 1
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 36'
GO

--(4 rows affected) 00:00:00 03/30/25  8:51:37 AM 
--select * from #duplicate_sasname

--select * FROM [dbo].[SAP_Naming] where [TABLE] = 'BSAK' and SasName=  'AmtInForeignCurrForTaxBreakdown'

--======================================
--Get column names from stage table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #bvorname
SELECT [name] stgname INTO #bvorname
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.stg_bvor') --@@ChangeMe if required
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 37'
GO
--select * from #bvorname
--(19 rows affected) 00:00:00 04/03/25 11:38:59 AM 

--======================================
--Get SAP column names
--======================================
-- If duplicate sasname then we are concatenate sasname_field column
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #SAP_Naming
CREATE TABLE #SAP_Naming 
( id int identity(1,1),
[stgname] VARCHAR(100),
[sasname]  VARCHAR(100),
[Datatype]  VARCHAR(100)
)
INSERT INTO #SAP_Naming ([stgname])
SELECT stgname from #bvorname

UPDATE sap SET sap.sasname = CASE WHEN c.[sasname] IS NULL THEN b.[sasname] ELSE b.[sasname] +'_'+b.[field]  END,
sap.Datatype = CASE  WHEN b.[datatype] IN ('NUMC') THEN 'INT' 
WHEN b.[datatype] IN ('CURR') THEN 'DECIMAL('+b.[Length]+',2)' 
WHEN b.[datatype] = 'DATS' THEN 'DATE'  WHEN b.[datatype] IN ('CLNT','CHAR','CUKY') THEN 'VARCHAR('+b.[Length]+')' ELSE 'VARCHAR(100)' END 
from #SAP_Naming sap LEFT JOIN  [dbo].[SAP_Naming]  b ON b.[field] = stgname
LEFT JOIN #duplicate_sasname c ON c.sasname = b.sasname
WHERE b.[TABLE] = 'BVOR' 
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 38'
GO

--(6 rows affected) 00:00:00 04/03/25 11:40:03 AM 

UPDATE #SAP_Naming SET sasname = stgname , Datatype = 'VARCHAR(100)' WHERE sasname IS NULL

UPDATE #SAP_Naming SET Datatype = 'DATETIME' where sasname = 'SAS_IMPORT_DATETIME'

--select * from #SAP_Naming order by 2

--======================================
--Create target table from stg table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DECLARE
@BVORTargetTable NVARCHAR(256) = 'bvor'

DECLARE @Max_ordinal_position varchar(100) = (SELECT MAX(id) FROM #SAP_Naming)
--select @Max_ordinal_position
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @columnDefinitions VARCHAR(MAX) = ''
-- Get column definitions
SELECT @columnDefinitions += '[' + [SasName] + '] ' + [datatype] + 
CASE WHEN id = @Max_ordinal_position THEN '' ELSE ',' END + CHAR(10)
FROM #SAP_Naming ORDER BY id

--print @columnDefinitions

-- Construct the CREATE TABLE statement
SET @sql = 'CREATE TABLE dbo.' + @BVORTargetTable + ' (' + CHAR(10) + @columnDefinitions 
 +',[SASREFNBR] [bigint] IDENTITY(1,1) NOT NULL,
 PRIMARY KEY CLUSTERED 
(
	[SASREFNBR] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
);
ALTER TABLE '+@BVORTargetTable+' ADD  DEFAULT (getdate()) FOR [SAS_IMPORT_DATETIME]
'
--PRINT @sql
exec (@sql);
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 39'
GO
--(0 rows affected) 00:00:00 03/30/25  8:52:23 AM 

--======================================
--Insert stg table data to Target table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
declare @BVORSourceTablename varchar(max) = 'dbo.stg_bvor'    --- @@ChangeMe if required
declare @BVORTargetTablename varchar(max) = 'dbo.bvor'   --- @@ChangeMe if required

DECLARE @BVORqry VARCHAR(MAX) =

+'CREATE OR ALTER PROCEDURE [dbo].[usp_build_'+REPLACE(@BVORSourceTablename,'dbo.stg_','')+']'+ CHAR(10)   
+'AS  ' + CHAR(10)   
+'BEGIN' + CHAR(10)  
+ CHAR(10)
+'INSERT INTO '+@BVORTargetTablename +'(' +STUFF((
    SELECT ', ' + char(13) + char(10) + QUOTENAME(c.COLUMN_NAME) 
    FROM INFORMATION_SCHEMA.COLUMNS c  WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @BVORTargetTablename and  QUOTENAME(c.COLUMN_NAME) NOT LIKE '%SASREFNBR%'
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10)+')'
  + CHAR(10)  
+'SELECT  ' + STUFF((
     SELECT ', ' + char(13) + char(10) +'['+sasname+']'+'	=	' 
	+CASE WHEN datatype = 'INT' THEN +'ISNULL(CAST('+stgname+' AS '+datatype+'),0)'
	  WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 1 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	 WHEN datatype LIKE '%DECIMAL%' THEN +'CAST(LEFT('+stgname+',15)' + ' AS ' + datatype + ')'
	ELSE 'CAST(' + stgname + ' AS ' + datatype + ')' END
    FROM #SAP_Naming
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10) + ' FROM ' + @BVORSourceTablename  --- Add additional where clause if required to filter invalid records
  + CHAR(10) + 'END';
--print @BVORqry
exec(@BVORqry)
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 40'
GO





--======================================
--step09 - adr6
--======================================
--select distinct [table] FROM [dbo].[SAP_Naming]  where [Table] = 'BSAK' and sasname = 'AmtInForeignCurrForTaxBreakdown'
--======================================
--Check sasname duplicate
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #duplicate_sasname
SELECT sasname INTO #duplicate_sasname FROM [dbo].[SAP_Naming] 
WHERE [TABLE] = 'ADR'
GROUP BY sasname HAVING COUNT(*) > 1
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 36'
GO

--(4 rows affected) 00:00:00 03/30/25  8:51:37 AM 
--select * from #duplicate_sasname

--select * FROM [dbo].[SAP_Naming] where [TABLE] = 'BSAK' and SasName=  'AmtInForeignCurrForTaxBreakdown'

--======================================
--Get column names from stage table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #adrname
SELECT [name] stgname INTO #adrname
FROM sys.columns
WHERE object_id = OBJECT_ID('dbo.stg_adr6') --@@ChangeMe if required
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 37'
GO
--select * from #bvorname
--(29 rows affected) 00:00:00 04/03/25 12:05:08 PM 

--======================================
--Get SAP column names
--======================================
-- If duplicate sasname then we are concatenate sasname_field column
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DROP TABLE IF EXISTS #SAP_Naming
CREATE TABLE #SAP_Naming 
( id int identity(1,1),
[stgname] VARCHAR(100),
[sasname]  VARCHAR(100),
[Datatype]  VARCHAR(100)
)
INSERT INTO #SAP_Naming ([stgname])
SELECT stgname from #adrname

UPDATE sap SET sap.sasname = CASE WHEN c.[sasname] IS NULL THEN b.[sasname] ELSE b.[sasname] +'_'+b.[field]  END,
sap.Datatype = CASE  WHEN b.[datatype] IN ('NUMC') THEN 'INT' 
WHEN b.[datatype] IN ('CURR') THEN 'DECIMAL('+b.[Length]+',2)' 
WHEN b.[datatype] = 'DATS' THEN 'DATE'  WHEN b.[datatype] IN ('CLNT','CHAR','CUKY') THEN 'VARCHAR('+b.[Length]+')' ELSE 'VARCHAR(100)' END 
from #SAP_Naming sap LEFT JOIN  [dbo].[SAP_Naming]  b ON b.[field] = stgname
LEFT JOIN #duplicate_sasname c ON c.sasname = b.sasname
WHERE b.[TABLE] = 'adr6' 
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 38'
GO

--(6 rows affected) 00:00:00 04/03/25 11:40:03 AM 

UPDATE #SAP_Naming SET sasname = stgname , Datatype = 'VARCHAR(100)' WHERE sasname IS NULL

UPDATE #SAP_Naming SET Datatype = 'DATETIME' where sasname = 'SAS_IMPORT_DATETIME'

--select * from #SAP_Naming order by 2

--======================================
--Create target table from stg table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
DECLARE
@ADR6TargetTable NVARCHAR(256) = 'adr6'

DECLARE @Max_ordinal_position varchar(100) = (SELECT MAX(id) FROM #SAP_Naming)
--select @Max_ordinal_position
DECLARE @sql VARCHAR(MAX) = ''
DECLARE @columnDefinitions VARCHAR(MAX) = ''
-- Get column definitions
SELECT @columnDefinitions += '[' + [SasName] + '] ' + [datatype] + 
CASE WHEN id = @Max_ordinal_position THEN '' ELSE ',' END + CHAR(10)
FROM #SAP_Naming ORDER BY id

--print @columnDefinitions

-- Construct the CREATE TABLE statement
SET @sql = 'CREATE TABLE dbo.' + @ADR6TargetTable + ' (' + CHAR(10) + @columnDefinitions 
 +',[SASREFNBR] [bigint] IDENTITY(1,1) NOT NULL,
 PRIMARY KEY CLUSTERED 
(
	[SASREFNBR] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
);
ALTER TABLE '+@ADR6TargetTable+' ADD  DEFAULT (getdate()) FOR [SAS_IMPORT_DATETIME]
'
--PRINT @sql
exec (@sql);
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 39'
GO
--(0 rows affected) 00:00:00 03/30/25  8:52:23 AM 

--======================================
--Insert stg table data to Target table
--======================================
EXEC MASTER.[DBO].[SP_INITTIME]
GO
declare @adrSourceTablename varchar(max) = 'dbo.stg_adr6'    --- @@ChangeMe if required
declare @adrTargetTablename varchar(max) = 'dbo.adr6'   --- @@ChangeMe if required

DECLARE @adrqry VARCHAR(MAX) =

+'CREATE OR ALTER PROCEDURE [dbo].[usp_build_'+REPLACE(@adrSourceTablename,'dbo.stg_','')+']'+ CHAR(10)   
+'AS  ' + CHAR(10)   
+'BEGIN' + CHAR(10)  
+ CHAR(10)
+'INSERT INTO '+@adrTargetTablename +'(' +STUFF((
    SELECT ', ' + char(13) + char(10) + QUOTENAME(c.COLUMN_NAME) 
    FROM INFORMATION_SCHEMA.COLUMNS c  WHERE TABLE_SCHEMA + '.' + TABLE_NAME = @adrTargetTablename and  QUOTENAME(c.COLUMN_NAME) NOT LIKE '%SASREFNBR%'
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10)+')'
  + CHAR(10)  
+'SELECT  ' + STUFF((
     SELECT ', ' + char(13) + char(10) +'['+sasname+']'+'	=	' 
	+CASE WHEN datatype = 'INT' THEN +'ISNULL(CAST('+stgname+' AS '+datatype+'),0)'
	  WHEN datatype = 'DATE' THEN +'CASE WHEN ISDATE('+stgname+') = 1 THEN CAST('+stgname+' AS DATE) ELSE ''1900-01-01'' END'
	 WHEN datatype LIKE '%DECIMAL%' THEN +'CAST(LEFT('+stgname+',15)' + ' AS ' + datatype + ')'
	ELSE 'CAST(' + stgname + ' AS ' + datatype + ')' END
    FROM #SAP_Naming
    FOR XML PATH(''), TYPE).value('.','nvarchar(max)'),1,2,'') +
  + char(13) + char(10) + ' FROM ' + @adrSourceTablename  --- Add additional where clause if required to filter invalid records
  + CHAR(10) + 'END';
--print @adrqry
exec(@adrqry)
GO
EXEC MASTER.DBO.SP_ENDTIME @@ROWCOUNT
PRINT 'Print 40'
GO