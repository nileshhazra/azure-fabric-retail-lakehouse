-- ==================================================================================
-- Enterprise Hybrid Lakehouse: Transactional Database Initialization
-- Target: Azure SQL Database (Serverless Tier)
-- Description: Creates source schema with incremental watermarking support,
--              realistic retail relationships, and optimized indexing.
-- ==================================================================================

-- 1. Create Control Table for Metadata-Driven Ingestion / Watermarking
IF OBJECT_ID('dbo.WatermarkControl', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.WatermarkControl (
        TableName VARCHAR(100) NOT NULL PRIMARY KEY,
        WatermarkColumn VARCHAR(100) NOT NULL,
        LastProcessedWatermark DATETIME2 NOT NULL,
        LastUpdatedTimestamp DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

-- 2. Create Master Customer Table (Target for SCD Type 2 in Silver Layer)
IF OBJECT_ID('dbo.Customers', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Customers (
        CustomerID INT IDENTITY(1001, 1) PRIMARY KEY,
        CustomerCode VARCHAR(30) NOT NULL UNIQUE,
        FirstName NVARCHAR(50) NOT NULL,
        LastName NVARCHAR(50) NOT NULL,
        Email NVARCHAR(100) NOT NULL,
        PhoneNumber NVARCHAR(25) NULL,
        CustomerSegment VARCHAR(30) NOT NULL DEFAULT 'Standard', -- Standard, Silver, Gold, Platinum
        City NVARCHAR(50) NOT NULL,
        State NVARCHAR(50) NOT NULL,
        PostalCode NVARCHAR(20) NOT NULL,
        Country NVARCHAR(50) NOT NULL DEFAULT 'United States',
        CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        ModifiedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE NONCLUSTERED INDEX IX_Customers_ModifiedDate 
    ON dbo.Customers (ModifiedDate) 
    INCLUDE (CustomerCode, CustomerSegment, City);
END
GO

-- 3. Create Product Catalog Table
IF OBJECT_ID('dbo.Products', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Products (
        ProductID INT IDENTITY(501, 1) PRIMARY KEY,
        ProductSKU VARCHAR(50) NOT NULL UNIQUE,
        ProductName NVARCHAR(100) NOT NULL,
        Category VARCHAR(50) NOT NULL,
        SubCategory VARCHAR(50) NOT NULL,
        UnitCost DECIMAL(18, 2) NOT NULL,
        UnitPrice DECIMAL(18, 2) NOT NULL,
        IsActive BIT NOT NULL DEFAULT 1,
        CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        ModifiedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );

    CREATE NONCLUSTERED INDEX IX_Products_ModifiedDate 
    ON dbo.Products (ModifiedDate)
    INCLUDE (ProductSKU, Category, UnitPrice);
END
GO

-- 4. Create Sales Orders Table (Target for Incremental Ingestion)
IF OBJECT_ID('dbo.SalesOrders', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SalesOrders (
        OrderID INT IDENTITY(100001, 1) PRIMARY KEY,
        OrderNumber VARCHAR(50) NOT NULL UNIQUE,
        CustomerID INT NOT NULL,
        StoreLocation VARCHAR(100) NOT NULL, -- e.g., 'Store-Dallas-01', 'Online-Web', 'Store-Chicago-04'
        OrderStatus VARCHAR(25) NOT NULL DEFAULT 'Pending', -- Pending, Processing, Shipped, Delivered, Cancelled
        PaymentMethod VARCHAR(30) NOT NULL DEFAULT 'Credit Card',
        TaxAmount DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
        ShippingCost DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
        TotalAmount DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
        OrderDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        ModifiedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_SalesOrders_Customers FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
    );

    -- Composite index to support delta queries in ADF
    CREATE NONCLUSTERED INDEX IX_SalesOrders_ModifiedDate 
    ON dbo.SalesOrders (ModifiedDate)
    INCLUDE (OrderNumber, CustomerID, StoreLocation, OrderStatus, TotalAmount);
END
GO

-- 5. Create Order Line Items Table
IF OBJECT_ID('dbo.OrderLineItems', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.OrderLineItems (
        LineItemID INT IDENTITY(1, 1) PRIMARY KEY,
        OrderID INT NOT NULL,
        ProductID INT NOT NULL,
        Quantity INT NOT NULL CHECK (Quantity > 0),
        UnitPrice DECIMAL(18, 2) NOT NULL,
        DiscountAmount DECIMAL(18, 2) NOT NULL DEFAULT 0.00,
        LineTotal AS (Quantity * UnitPrice - DiscountAmount) PERSISTED,
        CreatedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        ModifiedDate DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_OrderLineItems_SalesOrders FOREIGN KEY (OrderID) REFERENCES dbo.SalesOrders(OrderID),
        CONSTRAINT FK_OrderLineItems_Products FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID)
    );

    CREATE NONCLUSTERED INDEX IX_OrderLineItems_ModifiedDate 
    ON dbo.OrderLineItems (ModifiedDate)
    INCLUDE (OrderID, ProductID, Quantity, LineTotal);
END
GO

-- 6. Stored Procedures for ADF Watermarking Management

-- Get Current Watermark
CREATE OR ALTER PROCEDURE dbo.usp_GetWatermark
    @TableName VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT 
        TableName,
        WatermarkColumn,
        LastProcessedWatermark
    FROM dbo.WatermarkControl
    WHERE TableName = @TableName;
END
GO

-- Update Watermark After Successful Ingestion
CREATE OR ALTER PROCEDURE dbo.usp_UpdateWatermark
    @TableName VARCHAR(100),
    @NewWatermarkValue DATETIME2
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.WatermarkControl
    SET 
        LastProcessedWatermark = @NewWatermarkValue,
        LastUpdatedTimestamp = SYSUTCDATETIME()
    WHERE TableName = @TableName;
END
GO

-- 7. Seed Initial Watermark State (Starting point before data loads)
MERGE dbo.WatermarkControl AS target
USING (VALUES 
    ('Customers', 'ModifiedDate', '1900-01-01 00:00:00'),
    ('Products', 'ModifiedDate', '1900-01-01 00:00:00'),
    ('SalesOrders', 'ModifiedDate', '1900-01-01 00:00:00'),
    ('OrderLineItems', 'ModifiedDate', '1900-01-01 00:00:00')
) AS source (TableName, WatermarkColumn, LastProcessedWatermark)
ON target.TableName = source.TableName
WHEN MATCHED THEN 
    UPDATE SET target.WatermarkColumn = source.WatermarkColumn
WHEN NOT MATCHED THEN
    INSERT (TableName, WatermarkColumn, LastProcessedWatermark, LastUpdatedTimestamp)
    VALUES (source.TableName, source.WatermarkColumn, source.LastProcessedWatermark, SYSUTCDATETIME());
GO

PRINT '>>> Initialization complete: Tables, Indexes, and Watermark procedures created successfully. <<<';
