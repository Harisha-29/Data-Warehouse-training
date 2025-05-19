CREATE DATABASE EmployeeManagementDB;
GO

USE EmployeeManagementDB;
GO

CREATE TABLE EmployeeAttendance (
    AttendanceID INT PRIMARY KEY,
    EmployeeName VARCHAR(100),
    Department VARCHAR(50),
    Date DATE,
    Status VARCHAR(20),
    HoursWorked INT);

INSERT INTO EmployeeAttendance VALUES
(1, 'John Doe', 'IT', '2025-05-01', 'Present', 8),
(2, 'Priya Singh', 'HR', '2025-05-01', 'Absent', 0),
(3, 'Ali Khan', 'IT', '2025-05-01', 'Present', 7),
(4, 'Riya Patel', 'Sales', '2025-05-01', 'Late', 6),
(5, 'David Brown', 'Marketing', '2025-05-01', 'Present', 8);


-------------Tasks: 1. CRUD Operations:----------------------

------------Add a new attendance record:
-----------1.] Insert a record for Neha Sharma, from Finance, on 2025-05-01, marked as Present, with 8 hours worked.

INSERT INTO EmployeeAttendance VALUES (6, 'Neha Sharma', 'Finance', '2025-05-01', 'Present', 8);

----------2.] Update attendance status:
-----------Change Riya Patel's status from Late to Present.

UPDATE EmployeeAttendance
SET Status = 'Present'
WHERE EmployeeName = 'Riya Patel' AND Date = '2025-05-01';

----------3.] Delete a record:
----------Remove the attendance entry for Priya Singh on 2025-05-01.

Delete from EmployeeAttendance
WHERE EmployeeName = 'Priya Singh' AND Date = '2025-05-01';

-----------4.] Read all records:
----------Display all attendance records sorted by EmployeeName in ascending order.

SELECT * FROM EmployeeAttendance
order by EmployeeName ASC;


----------- TASK: 2. Sorting and Filtering:------------

-----------5.] Sort by Hours Worked:
---------List employees sorted by HoursWorked in descending order.

SELECT * FROM EmployeeAttendance
order by HoursWorked DESC;

--------6.] Filter by Department:
----------Display all attendance records for the IT department.

SELECT * FROM EmployeeAttendance
WHERE Department = 'IT';

--------7.] Filter with AND condition:
-------List all Present employees from the IT department.

SELECT * FROM EmployeeAttendance
WHERE Department = 'IT' AND Status = 'Present';

----------8.] Filter with OR condition:
---------Retrieve all employees who are either Absent or Late.

SELECT * FROM EmployeeAttendance
WHERE Status = 'Absent' OR Status = 'Late';


------------TASK: 3. Aggregation and Grouping:-----

------------9.] Total Hours Worked by Department:
-----------Calculate the total hours worked grouped by Department.

SELECT Department, SUM(HoursWorked) AS TotalHoursWorked
FROM EmployeeAttendance
GROUP BY Department;


------------10.] Average Hours Worked:
----------Find the average hours worked per day across all departments.

SELECT AVG(HoursWorked) AS AverageHoursWorked
FROM EmployeeAttendance;

----------11.] Attendance Count by Status:
-----------Count how many employees were Present, Absent, or Late.

SELECT Status, COUNT(*) AS Count
FROM EmployeeAttendance
GROUP BY Status;

------------Task: 4. Conditional and Pattern Matching:---------

------------12.] Find employees by name prefix:
-------------List all employees whose EmployeeName starts with 'R'.

SELECT * FROM EmployeeAttendance
WHERE EmployeeName LIKE 'R%';


------------13.] Filter by multiple conditions:
-------------Display employees who worked more than 6 hours and are marked Present.

SELECT * FROM EmployeeAttendance
WHERE HoursWorked > 6 AND Status = 'Present';


----------14.] Filter using BETWEEN operator:
----------List employees who worked between 6 and 8 hours.

SELECT * FROM EmployeeAttendance
WHERE HoursWorked BETWEEN 6 AND 8;



-------------TASK: 5. Advanced Queries:----------------

-----------15.] Top 2 employees with the most hours:
----------Display the top 2 employees with the highest number of hours worked.

SELECT TOP 2 * FROM EmployeeAttendance
ORDER BY HoursWorked DESC;

----------16.] Employees who worked less than the average hours:
----------List all employees whose HoursWorked are below the average.

SELECT * FROM EmployeeAttendance
WHERE HoursWorked < (
    SELECT AVG(HoursWorked) FROM EmployeeAttendance
);

--------17.] Group by Status:
--------Calculate the average hours worked for each attendance status (Present, Absent, Late).

SELECT Status, AVG(HoursWorked) AS AverageHoursWorked
FROM EmployeeAttendance
GROUP BY Status;

----18.]Find duplicate entries:
------Identify any employees who have multiple attendance records on the same date.

SELECT EmployeeName, Date, COUNT(*) AS DuplicateCount
FROM EmployeeAttendance
GROUP BY EmployeeName, Date
HAVING COUNT(*) > 1;


-------TASK:6. Join and Subqueries (if related tables are present):------

------19.] Department with most Present employees:
-----------Find the department with the highest number of Present employees.
SELECT TOP 1 Department, COUNT(*) AS PresentCount
FROM EmployeeAttendance
WHERE Status = 'Present'
GROUP BY Department
ORDER BY PresentCount DESC;


-------20.] Employee with maximum hours per department:
------------Find the employee with the most hours worked in each department.

SELECT ea.*
FROM EmployeeAttendance ea
JOIN (
    SELECT Department, MAX(HoursWorked) AS MaxHours
    FROM EmployeeAttendance
    GROUP BY Department
) deptMax
ON ea.Department = deptMax.Department AND ea.HoursWorked = deptMax.MaxHours;



