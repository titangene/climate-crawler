USE [Test_DB]
GO

/****** Object:  Table [dbo].[climate_crawler_log]    Script Date: 2018/8/23 下午 04:18:19 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[climate_crawler_log](
	[Station_ID] [char](6) NOT NULL,
	[Station_Area] [nchar](32) NOT NULL,
	[Reporttime] [datetime] NOT NULL,
	[Hourly_Start_Period] [char](10) NULL,
	[Hourly_End_Period] [char](10) NULL,
	[Daily_Start_Period] [char](10) NULL,
	[Daily_End_Period] [char](10) NULL,
	PRIMARY KEY ([Station_ID])
)
GO
