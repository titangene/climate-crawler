USE [Test_DB]
GO

/****** Object:  Table [dbo].[Hourly_Climate_data]    Script Date: 2018/8/23 下午 04:23:44 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Hourly_Climate_data](
	[UUID] [nchar](80) NOT NULL,
	[Area] [nchar](32) NULL,
	[Temperature] [float] NULL,
	[Humidity] [float] NULL,
	[SunShine_hr] [float] NULL,
	[SunShine_MJ] [float] NULL,
	[Reporttime] [datetime] NULL,
	PRIMARY KEY ([UUID])
)
GO

