USE [Test_DB]
GO

/****** Object:  Table [dbo].[climate_crawler_log]    Script Date: 2018/8/23 下午 04:18:19 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[climate_crawler_log](
	[Climate_Type] [char](6) NOT NULL,
	[Reporttime] [datetime] NULL,
	[Start_Period] [date] NULL,
	[End_Period] [date] NULL
) ON [PRIMARY]
GO
