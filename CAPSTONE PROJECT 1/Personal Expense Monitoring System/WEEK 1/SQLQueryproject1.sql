CREATE DATABASE PersonalExpenseDB;
GO

USE PersonalExpenseDB;
GO

CREATE TABLE Users (
    user_id INT PRIMARY KEY IDENTITY(1,1),
    name NVARCHAR(100),
    email NVARCHAR(100) UNIQUE,
    created_at DATETIME DEFAULT GETDATE()
);

CREATE TABLE Categories (
    category_id INT PRIMARY KEY IDENTITY(1,1),
    category_name NVARCHAR(100) UNIQUE
);

CREATE TABLE Expenses (
    expense_id INT PRIMARY KEY IDENTITY(1,1),
    user_id INT,
    category_id INT,
    amount DECIMAL(10,2),
    expense_date DATE,
    description NVARCHAR(255),
    FOREIGN KEY (user_id) REFERENCES Users(user_id),
    FOREIGN KEY (category_id) REFERENCES Categories(category_id)
);

-- Insert Users
INSERT INTO Users (name, email) VALUES ('Alice', 'alice@example.com');
INSERT INTO Users (name, email) VALUES ('Bob', 'bob@example.com');

-- Insert Categories
INSERT INTO Categories (category_name) VALUES ('Groceries');
INSERT INTO Categories (category_name) VALUES ('Utilities');
INSERT INTO Categories (category_name) VALUES ('Rent');

-- Insert Expenses
INSERT INTO Expenses (user_id, category_id, amount, expense_date, description)
VALUES (1, 1, 120.50, '2025-05-10', 'Supermarket shopping');

INSERT INTO Expenses (user_id, category_id, amount, expense_date, description)
VALUES (1, 2, 75.00, '2025-05-15', 'Electricity bill');

UPDATE Expenses
SET amount = 130.75
WHERE expense_id = 1;

DELETE FROM Expenses
WHERE expense_id = 2;

CREATE PROCEDURE GetMonthlyExpenseSummary
    @user_id INT,
    @month INT,
    @year INT
AS
BEGIN
    SELECT 
        c.category_name,
        SUM(e.amount) AS total_amount
    FROM Expenses e
    JOIN Categories c ON e.category_id = c.category_id
    WHERE e.user_id = @user_id
        AND MONTH(e.expense_date) = @month
        AND YEAR(e.expense_date) = @year
    GROUP BY c.category_name;
END;

EXEC GetMonthlyExpenseSummary @user_id = 1, @month = 5, @year = 2025;






