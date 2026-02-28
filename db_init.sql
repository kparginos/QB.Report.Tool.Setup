-- Create database
CREATE DATABASE QXLExportDB
ON 
(
    NAME = QXLExportDB_Data,
    FILENAME = '/var/opt/mssql/data/QXLExportDB.mdf',
    SIZE = 20MB,
    MAXSIZE = 100MB,
    FILEGROWTH = 5MB
)
LOG ON
(
    NAME = MyDatabase_Log,
    FILENAME = '/var/opt/mssql/data/QXLExportDB_log.ldf',
    SIZE = 10MB,
    MAXSIZE = 50MB,
    FILEGROWTH = 5MB
)
GO

-- Create tables
USE [QXLExportDB]
GO

/****** Object:  Table [dbo].[FileData]    Script Date: 09/02/2026 18:41:11 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[FileData](
	[AccNum] [varchar](6) NOT NULL,
	[AccLvl] [int] NOT NULL,
	[AccGrp] [varchar](3) NOT NULL,
	[Descr] [varchar](100) NULL,
	[Debit] [decimal(18,2)] NOT NULL,
	[Credit] [decimal(18,2)] NOT NULL,
	[ImportDt] [datetime] NOT NULL,
	[RefDt] [datetime] NOT NULL,
	[CompanyName] [varchar](100) NOT NULL,
 CONSTRAINT [PK_FileData] PRIMARY KEY CLUSTERED 
(
	[AccNum] ASC,
	[ImportDt] ASC,
	[RefDt] ASC,
	[CompanyName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

/****** Object:  Index [idxFileDataUploads_ImportDate]    Script Date: 09/02/2026 18:49:17 ******/
CREATE NONCLUSTERED INDEX [idxFileData_ImpDtRefDtCompNam] ON [dbo].[FileData]
(
	ImportDt ASC,
	RefDt ASC,
	CompanyName ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

Create Table dbo.FileDataUploads
(
	FileDataUploadsID UNIQUEIDENTIFIER  NOT NULL,
	FilePath VarChar(1024) NOT NULL,
	ImportUser VarChar(50) NOT NULL,
	ImportDate Datetime NOT NULL,
	RefDt Datetime NOT NULL,
	CompanyName VarChar(100) NOT NULL,
	CONSTRAINT [PK_FileDataUploads] PRIMARY KEY CLUSTERED 
	(
		FileDataUploadsID ASC
	)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO

/****** Object:  Index [idxFileDataUploads_ImportDate]    Script Date: 09/02/2026 18:49:17 ******/
CREATE NONCLUSTERED INDEX [idxFileDataUploads_ImportDate] ON [dbo].[FileDataUploads]
(
	[ImportDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

/****** Object:  Index [idxFileDataUploads_ImportDate]    Script Date: 09/02/2026 18:49:17 ******/
CREATE NONCLUSTERED INDEX [idxFileDataUploads_RefDt] ON [dbo].[FileDataUploads]
(
	RefDt ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
GO

-- Create sp
Use QXLExportDB
GO

Create Or Alter Procedure sp_CalculateTotals(@ImportDt DateTime, @CompanyName VarChar(100)) As
Begin
	Declare @accNumLow VarChar(6)
	Declare @accNumUpper VarChar(6)
	Declare @lvl4CrSum Decimal(18,2)
	Declare @lvl4DrSum Decimal(18,2)
	Declare @lvl3CrSum Decimal(18,2)
	Declare @lvl3DrSum Decimal(18,2)
	Declare @lvl2CrSum Decimal(18,2)
	Declare @lvl2DrSum Decimal(18,2)
	Declare @hasLvl2 Int
	Declare @hasLvl3 Int

	Declare @AccNum VarChar(6)
	Declare @AccLvl Int
	Declare @AccGrp VarChar(3)
	Declare @Descr VarChar(100)
	Declare @Credit Decimal(18,2)
	Declare @Debit Decimal(18,2)
	Declare @ImpDt Datetime
	Declare @RefDt Datetime
	Declare @ErrorMsg VarChar(255)

	If Not Exists(Select 1 From FileData Where ImportDt = @ImportDt And CompanyName = @CompanyName)
	Begin
		Set @ErrorMsg = 'Import date <<' + Convert(VarChar(10), @ImportDt, 103) + '>> for company <<' + Ltrim(RTrim(@CompanyName))  + '>> not found.';
		Throw 50001, @ErrorMsg, 1;
	End

	Create Table #ExportData
	(
		AccNum VarChar(6),
		AccLvl Int,
		AccGrp VarChar(3),
		Descr VarChar(100),
		Debit Decimal(18,2),
		Credit Decimal(18,2),
		ImportDt Datetime,
		RefDt Datetime,
		CompanyName VarChar(100)
	)

	Create Table #Ranges
	(
		LowerAcc VarChar(6),
		UpperAcc VarChar(6)
	)

	Insert Into #Ranges
		SELECT
			AccNum from_value,
			COALESCE(LEAD(AccNum) OVER (ORDER BY AccNum), 999999) AS to_value
		FROM (Select AccNum
				From  [dbo].[FileData]
				Where ImportDt = @ImportDt
				And AccLvl = 1
				And CompanyName = @CompanyName) R
		ORDER BY AccNum

	Declare crRanges Cursor For
		Select *
		From #Ranges
	Open crRanges
	Fetch Next From crRanges Into @accNumLow, @accNumUpper
	While @@FETCH_STATUS = 0
	Begin
		Print 'Calculating block between ' + @accNumLow + ' And ' + @accNumUpper 
		Declare crBlock Cursor For
			Select	AccNum, AccLvl, AccGrp, Descr, Debit, Credit, ImportDt, RefDt
			From	[dbo].[FileData]
			Where	ImportDt = @ImportDt
			And		AccNum >= @accNumLow 
			And		AccNum < @accNumUpper
			And		CompanyName = @CompanyName
			Order by AccNum DESC
		Open crBlock
		Fetch Next From crBlock Into @AccNum, @AccLvl, @AccGrp, @Descr, @Debit, @Credit, @ImpDt, @RefDt
		Select @lvl2CrSum = 0
		Select @lvl2DrSum = 0
		Select @lvl3CrSum = 0
		Select @lvl3DrSum = 0
		Select @lvl4CrSum = 0
		Select @lvl4DrSum = 0
		Select @hasLvl2 = 0
		Select @hasLvl3 = 0
		While @@FETCH_STATUS =0
		Begin
			If @AccLvl = 4
			Begin
				Select @lvl4CrSum = @lvl4CrSum + @Credit
				Select @lvl4DrSum = @lvl4DrSum + @Debit
				Insert Into #ExportData Values(@AccNum, @AccLvl, @AccGrp, @Descr, @Debit, @Credit, @ImpDt, @RefDt, @CompanyName)
				Print 'Level 4 found... Db:' + Convert(VarChar(MAX), @Debit) + ' - Cr:' + Convert(VarChar(MAX), @Credit)
			End
			If @AccLvl = 3
			Begin
				Insert Into #ExportData Values(@AccNum, @AccLvl, @AccGrp, @Descr, @lvl4DrSum, @lvl4CrSum, @ImpDt, @RefDt, @CompanyName)
				Select @lvl3CrSum = @lvl3CrSum + @lvl4CrSum
				Select @lvl3DrSum = @lvl3DrSum + @lvl4DrSum
				Print 'Level 3 found... Db:' + Convert(VarChar(MAX), @Debit) + ' - Cr:' + Convert(VarChar(MAX), @Credit)
				Select @lvl4CrSum = 0
				Select @lvl4CrSum = 0
				Select @hasLvl3 = 1
			End
			If @AccLvl = 2
			Begin
				If @hasLvl3 = 1
					Begin
						Print 'Level 3 Sums used: Db:' + Convert(VarChar(MAX), IsNull(@lvl3DrSum, 0)) + ' - Cr:' + Convert(VarChar(MAX), IsNull(@lvl3CrSum, 0))
						Insert Into #ExportData Values(@AccNum, @AccLvl, @AccGrp, @Descr, @lvl3DrSum, @lvl3CrSum, @ImpDt, @RefDt, @CompanyName)
						Select @lvl2CrSum = @lvl2CrSum + @lvl3CrSum
						Select @lvl2DrSum = @lvl2DrSum + @lvl3DrSum
					End
				Else
					Begin
						Print 'Level 4 Sums used: Db:' + Convert(VarChar(MAX), IsNull(@lvl4DrSum, 0)) + ' - Cr:' + Convert(VarChar(MAX), IsNull(@lvl4CrSum, 0))
						Insert Into #ExportData Values(@AccNum, @AccLvl, @AccGrp, @Descr, @lvl4DrSum, @lvl4CrSum, @ImpDt, @RefDt, @CompanyName)
						Select @lvl2CrSum = @lvl2CrSum + @lvl4CrSum
						Select @lvl2DrSum = @lvl2DrSum + @lvl4DrSum
					End
				Select @lvl3CrSum = 0
				Select @lvl3DrSum = 0
				Select @lvl4CrSum = 0
				Select @lvl4DrSum = 0
				Select @hasLvl3 = 0
				Select @hasLvl2 = 1
			End
			If @AccLvl = 1
			Begin
				Print 'Level 1 found. HasLvl2: ' + Convert(VarChar(1), @hasLvl2)
				If @hasLvl2 = 1
					Begin
						Print 'Level 2 Sums used: Db:' + Convert(VarChar(MAX), IsNull(@lvl2DrSum, 0)) + ' - Cr:' + Convert(VarChar(MAX), IsNull(@lvl2CrSum, 0))
						Insert Into #ExportData Values(@AccNum, @AccLvl, @AccGrp, @Descr, @lvl2DrSum, @lvl2CrSum, @ImpDt, @RefDt, @CompanyName)
					End
				Else
					Begin
						If @hasLvl3 = 1
							Begin
								Print 'Level 3 Sums used: Db:' + Convert(VarChar(MAX), IsNull(@lvl3DrSum, 0)) + ' - Cr:' + Convert(VarChar(MAX), IsNull(@lvl3CrSum, 0))
								Insert Into #ExportData Values(@AccNum, @AccLvl, @AccGrp, @Descr, @lvl3DrSum, @lvl3CrSum, @ImpDt, @RefDt, @CompanyName)
							End
						Else
							Begin
								Print 'Level 4 Sums used: Db:' + Convert(VarChar(MAX), IsNull(@lvl4DrSum, 0)) + ' - Cr:' + Convert(VarChar(MAX), IsNull(@lvl4CrSum, 0))
								Insert Into #ExportData Values(@AccNum, @AccLvl, @AccGrp, @Descr, @lvl4DrSum, @lvl4CrSum, @ImpDt, @RefDt, @CompanyName)
							End
					End
				Select @lvl2CrSum = 0
				Select @lvl2DrSum = 0
				Select @lvl3CrSum = 0
				Select @lvl3CrSum = 0
				Select @hasLvl2 = 0
			End
			Fetch Next From crBlock Into @AccNum, @AccLvl, @AccGrp, @Descr, @Debit, @Credit, @ImpDt, @RefDt
		End
		Close crBlock
		Deallocate crBlock
		
		Fetch Next From crRanges Into @accNumLow, @accNumUpper
	End
	Close crRanges
	Deallocate crRanges

	Select *
	From #ExportData
	Order by ImportDt, AccNum, AccLvl
End
GO
