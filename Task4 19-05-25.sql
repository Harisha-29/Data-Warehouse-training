CREATE DATABASE ProductInventoryDB;
GO

USE ProductInventoryDB;
GO

CREATE TABLE ProductInventory (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Quantity INT,
    UnitPrice DECIMAL(10, 2),
    Supplier VARCHAR(100),
    LastRestocked DATE
);

INSERT INTO ProductInventory (ProductID, ProductName, Category, Quantity, UnitPrice, Supplier, LastRestocked)
VALUES 
(1, 'Laptop', 'Electronics', 20, 70000, 'TechMart', '2025-04-20'),
(2, 'Office Chair', 'Furniture', 50, 5000, 'HomeComfort', '2025-04-18'),
(3, 'Smartwatch', 'Electronics', 30, 15000, 'GadgetHub', '2025-04-22'),
(4, 'Desk Lamp', 'Lighting', 80, 1200, 'BrightLife', '2025-04-25'),
(5, 'Wireless Mouse', 'Electronics', 100, 1500, 'GadgetHub', '2025-04-30');


------------Task:1. CRUD Operations:----------

------------------1.] Add a new product:
------------------Insert a product named "Gaming Keyboard", Category Electronics, Quantity 40, UnitPrice 3500, Supplier TechMart, LastRestocked 2025-05-01.

INSERT INTO ProductInventory (ProductID, ProductName, Category, Quantity, UnitPrice, Supplier, LastRestocked)
VALUES (6, 'Gaming Keyboard', 'Electronics', 40, 3500, 'TechMart', '2025-05-01');


------------2.] Update stock quantity:
---------------Increase the Quantity of Desk Lamp by 20.

UPDATE ProductInventory
SET Quantity = Quantity + 20
WHERE ProductName = 'Desk Lamp';

--------3.] Delete a discontinued product:
------------Remove the product with ProductID = 2 (Office Chair).

DELETE FROM ProductInventory
WHERE ProductID = 2;


--------4.] Read all products:
------------Display all products sorted by ProductName in ascending order.

SELECT * FROM ProductInventory
ORDER BY ProductName ASC;

------------Task:2 2. Sorting and Filtering:----------

------5.] Sort by Quantity:
---------List products sorted by Quantity in descending order.

SELECT * FROM ProductInventory
ORDER BY Quantity DESC;

--------6.] Filter by Category:
-----------Display all Electronics products.

SELECT * FROM ProductInventory
WHERE Category = 'Electronics';

---------7.] Filter with AND condition:
------------List all Electronics products with Quantity > 20.
SELECT * FROM ProductInventory
WHERE Category = 'Electronics' AND Quantity > 20;


---------8.] Filter with OR condition:
------------Retrieve all products that belong to Electronics or have a UnitPrice below 2000.

SELECT * FROM ProductInventory
WHERE Category = 'Electronics' OR UnitPrice < 2000;


--------------Task:3. Aggregation and Grouping:-------

--------------9.] Total stock value calculation:
----------------Calculate the total value of all products (Quantity * UnitPrice).

SELECT SUM(Quantity * UnitPrice) AS TotalStockValue
FROM ProductInventory;


-------------10.] Average price by category:
--------------Find the average price of products grouped by Category.
SELECT Category, AVG(UnitPrice) AS AveragePrice
FROM ProductInventory
GROUP BY Category;

-----------11.] Count products by supplier:
---------------Display the number of products supplied by GadgetHub.

SELECT COUNT(*) AS ProductCount
FROM ProductInventory
WHERE Supplier = 'GadgetHub';


--------------Task:4. Conditional and Pattern Matching:------------

-----12.] Find products by name prefix:
----------List all products whose ProductName starts with 'W'.
SELECT * FROM ProductInventory
WHERE ProductName LIKE 'W%';


--------13.]Filter by supplier and price:
-----------Display all products supplied by GadgetHub with a UnitPrice above 10000.
SELECT * FROM ProductInventory
WHERE Supplier = 'GadgetHub' AND UnitPrice > 10000;


---------14.] Filter using BETWEEN operator:
------------List all products with UnitPrice between 1000 and 20000.

SELECT * FROM ProductInventory
WHERE UnitPrice BETWEEN 1000 AND 20000;

-------------Task:5. Advanced Queries:-----------

-------15.]Top 3 most expensive products:
----------Display the top 3 products with the highest UnitPrice.
SELECT TOP 3 *
FROM ProductInventory
ORDER BY UnitPrice DESC;


---------16.] Products restocked recently:
----------Find all products restocked in the last 10 days.
SELECT *
FROM ProductInventory
WHERE LastRestocked >= DATEADD(DAY, -10, CAST(GETDATE() AS DATE));


----------17.] Group by Supplier:
-------------Calculate the total quantity of products from each Supplier.

SELECT Supplier, SUM(Quantity) AS TotalQuantity
FROM ProductInventory
GROUP BY Supplier;


-----------18.] Check for low stock:
--------------List all products with Quantity less than 30.

SELECT *
FROM ProductInventory
WHERE Quantity < 30;


--------------Task:6. Join and Subqueries: ---------------

---------19.] Supplier with most products:
-----------Find the supplier who provides the maximum number of products.
SELECT TOP 1 Supplier, COUNT(*) AS ProductCount
FROM ProductInventory
GROUP BY Supplier
ORDER BY ProductCount DESC;


-------------20.] Product with highest stock value:
------------Find the product with the highest total stock value (Quantity * UnitPrice).
SELECT TOP 1 *, (Quantity * UnitPrice) AS StockValue
FROM ProductInventory
ORDER BY StockValue DESC;

