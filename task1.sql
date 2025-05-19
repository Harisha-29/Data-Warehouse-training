CREATE DATABASE ProductDB;
GO

USE ProductDB;
GO

CREATE TABLE Product (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Price DECIMAL(10, 2),
    StockQuantity INT,
    Supplier VARCHAR(100)
);


INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier)
VALUES
(1, 'Laptop', 'Electronics', 70000, 50, 'TechMart'),
(2, 'Office Chair', 'Furniture', 5000, 100, 'HomeComfort'),
(3, 'Smartwatch', 'Electronics', 15000, 200, 'GadgetHub'),
(4, 'Desk Lamp', 'Lighting', 1200, 300, 'BrightLife'),
(5, 'Wireless Mouse', 'Electronics', 1500, 250, 'GadgetHub');

--------------Tasks:1. CRUD Operations:

---------------1.Add a new product:	Insert a product named "Gaming Keyboard" under the "Electronics" category, priced at 3500, with 150 units in stock, supplied by "TechMart".---------------

INSERT INTO Product (ProductID, ProductName, Category, Price, StockQuantity, Supplier)
VALUES (6, 'Gaming Keyboard', 'Electronics', 3500, 150, 'TechMart');

--------------2.Update product price: Increase the price of all Electronics products by 10%-------------

UPDATE Product
SET Price = Price * 1.10
WHERE Category = 'Electronics';

---------------3.Delete a product: Remove the product with the ProductID = 4 (Desk Lamp).----------------

DELETE FROM Product
WHERE ProductID = 4;

----------------4.Read all products:Display all products sorted by Price in descending order--------------


SELECT * FROM Product
ORDER BY Price DESC;

----------------Task 2. Sorting and Filtering:

---------------5.Sort products by stock quantity: Display the list of products sorted by StockQuantity in ascending order.

SELECT * FROM Product
ORDER BY StockQuantity ASC;


--------------6.Filter products by category: List all products belonging to the Electronics category.

SELECT * FROM Product
WHERE Category = 'Electronics';


-------------7.	Filter products with AND condition:Retrieve all Electronics products priced above 5000.

SELECT * FROM Product
WHERE Category = 'Electronics' AND Price > 5000;

--------------8.Filter products with OR condition:List all products that are either Electronics or priced below 2000.

SELECT * FROM Product
WHERE Category = 'Electronics' OR Price < 2000;


---------------------Task:3. Aggregation and Grouping:

-------------9.	Calculate total stock value: Find the total stock value (Price * StockQuantity) for all products.


SELECT SUM(Price * StockQuantity) AS TotalStockValue
FROM Product;

-------------10.Average price of each category: Calculate the average price of products grouped by Category.


SELECT Category, AVG(Price) AS AveragePrice
FROM Product
GROUP BY Category;

--------------11.Total number of products by supplier: Count the total number of products supplied by GadgetHub.


SELECT COUNT(*) AS TotalProductsByGadgetHub
FROM Product
WHERE Supplier = 'GadgetHub';



--------------Task 4. Conditional and Pattern Matching:-------------

------------12.Find products with a specific keyword: Display all products whose ProductName contains the word "Wireless".


SELECT * FROM Product
WHERE ProductName LIKE '%Wireless%';

-----------13.Search for products from multiple suppliers: Retrieve all products supplied by either TechMart or GadgetHub.


SELECT * FROM Product
WHERE Supplier IN ('TechMart', 'GadgetHub');


-------------14.Filter using BETWEEN operator:List all products with a price between 1000 and 20000.


SELECT * FROM Product
WHERE Price BETWEEN 1000 AND 20000;



----------------Task.5. Advanced Queries:--------------

-------------15.Products with high stock: Find products where StockQuantity is greater than the average stock quantity.


SELECT * FROM Product
WHERE StockQuantity > (
    SELECT AVG(StockQuantity) FROM Product);


-------------16.Get top 3 expensive products: Display the top 3 most expensive products in the table.


SELECT TOP 3 * FROM Product
ORDER BY Price DESC;


-------------17.Find duplicate supplier names: Identify any duplicate supplier names in the table.


SELECT Supplier, COUNT(*) AS ProductCount
FROM Product
GROUP BY Supplier
HAVING COUNT(*) > 1;


---------------18.Product summary: Generate a summary that shows each Category with the number of products and the total stock value.

SELECT 
    Category,
    COUNT(*) AS NumberOfProducts,
    SUM(Price * StockQuantity) AS TotalStockValue
FROM Product
GROUP BY Category;


--------------------Task 6. Join and Subqueries (if related tables are present)---------

---------------19.Supplier with most products: Find the supplier who provides the maximum number of products.


SELECT TOP 1 Supplier, COUNT(*) AS ProductCount
FROM Product
GROUP BY Supplier
ORDER BY ProductCount DESC;

---------------20.Most expensive product per category: List the most expensive product in each category.


SELECT p.*
FROM Product p
INNER JOIN (
    SELECT Category, MAX(Price) AS MaxPrice
    FROM Product
    GROUP BY Category
) maxPricePerCategory
ON p.Category = maxPricePerCategory.Category
AND p.Price = maxPricePerCategory.MaxPrice;
