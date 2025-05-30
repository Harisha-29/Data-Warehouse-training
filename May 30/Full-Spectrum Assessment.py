"""1. for num in range(11, 51, 2):
    print(num)

2. def is_leap_year(year):
    return year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)

 3. text = "Example string with a few a's"
count = text.count('a')
print(count)

4. keys = ['a', 'b', 'c']
values = [100, 200, 300]
my_dict = dict(zip(keys, values))
print(my_dict)

salaries = [50000, 60000, 55000, 70000, 52000]

# Maximum salary
max_salary = max(salaries)

# Average salary
average_salary = sum(salaries) / len(salaries)

# Salaries above average
above_average = [s for s in salaries if s > average_salary]

# Sorted in descending order
sorted_desc = sorted(salaries, reverse=True)

print("Max Salary:", max_salary)
print("Above Average Salaries:", above_average)
print("Descending Sorted Salaries:", sorted_desc)


6. a = [1, 2, 3, 4]
b = [3, 4, 5, 6]
set_a = set(a)
set_b = set(b)
print("Difference:", set_a - set_b)


7. class Employee:
    def __init__(self, employee_id, name, department, salary):
        self.employee_id = employee_id
        self.name = name
        self.department = department
        self.salary = salary

    def display(self):
        print(f"ID: {self.employee_id}, Name: {self.name}, Department: {self.department}, Salary: {self.salary}")

    def is_high_earner(self):
        return self.salary > 60000


8. class Employee:
    pass


class Project(Employee):
    def __init__(self, employee_id, name, department, salary, project_name, hours_allocated):
        super().__init__(employee_id, name, department, salary)
        self.project_name = project_name
        self.hours_allocated = hours_allocated

    def display_project(self):
        self.display()
        print(f"Project: {self.project_name}, Hours Allocated: {self.hours_allocated}")

9. class Employee:
    def __init__(self, employee_id, name, department, salary):
        self.employee_id = employee_id
        self.name = name
        self.department = department
        self.salary = salary

    def display(self):
        print(f"ID: {self.employee_id}, Name: {self.name}, Department: {self.department}, Salary: {self.salary}")

    def is_high_earner(self):
        return self.salary > 60000


# Instantiating employees
e1 = Employee(1, "Ali", "HR", 50000)
e2 = Employee(2, "Neha", "IT", 60000)
e3 = Employee(4, "Sara", "IT", 70000)

employees = [e1, e2, e3]

# Displaying info and checking high earners
for emp in employees:
    emp.display()
    print("High Earner:", emp.is_high_earner())
    print()

10.import csv

with open('employees.csv', 'r') as file:
    reader = csv.DictReader(file)
    it_employees = [row['Name'] for row in reader if row['Department'] == 'IT']

with open('it_employees.txt', 'w') as outfile:
    for name in it_employees:
        outfile.write(name + '\n')

print("IT employee names written to it_employees.txt")

11. def count_words_in_file(filename):
    with open(filename, 'r') as file:
        text = file.read()
        words = text.split()
        return len(words)

word_count = count_words_in_file('it_employees.txt')
print("Number of words in file:", word_count)

12. try:
    num = float(input("Enter a number: "))
    print("Square:", num ** 2)
except ValueError:
    print("Invalid input! Please enter a numeric value.")

13. def safe_divide(a, b):
    try:
        return a / b
    except ZeroDivisionError:
        return "Cannot divide by zero."

# Example
print(safe_divide(10, 2))  # 5.0
print(safe_divide(10, 0))  # Error message

14, 15, 16. import pandas as pd
from datetime import datetime
import os

if not os.path.exists('projects.csv') or os.stat('projects.csv').st_size == 0:
    with open('projects.csv', 'w') as f:
        f.write("ProjectID,EmployeeID,ProjectName,HoursAllocated\n")
        f.write("101,2,AI Chatbot,120\n")
        f.write("102,3,ERP System,200\n")
        f.write("103,3,Payroll Automation,150\n")
        f.write("104,5,Cloud Migration,100\n")

# Q14. Load CSVs
employees_df = pd.read_csv('employees.csv')
projects_df = pd.read_csv('projects.csv')

# Q15
print("First 2 rows of employees:")
print(employees_df.head(2))

print("\nUnique Departments:")
print(employees_df['Department'].unique())

print("\nAverage Salary by Department:")
print(employees_df.groupby('Department')['Salary'].mean())

# Q16. Add TenureInYears
current_year = datetime.now().year
employees_df['JoiningYear'] = pd.to_datetime(employees_df['JoiningDate']).dt.year
employees_df['TenureInYears'] = current_year - employees_df['JoiningYear']

print("\nEmployees with Tenure:")
print(employees_df[['Name', 'JoiningYear', 'TenureInYears']])


import pandas as pd

# Load CSV first
employees_df = pd.read_csv('employees.csv')

# Q17. Filter IT employees with salary > 60000
filtered_it = employees_df[(employees_df['Department'] == 'IT') & (employees_df['Salary'] > 60000)]
print("\nFiltered IT Employees with salary > 60000:")
print(filtered_it)

# Q18. Group by Department: Count, Total, Average Salary
grouped = employees_df.groupby('Department')['Salary'].agg(['count', 'sum', 'mean'])
print("\nDepartment-wise Salary Stats:")
print(grouped.rename(columns={'count': 'EmployeeCount', 'sum': 'TotalSalary', 'mean': 'AverageSalary'}))

# Q19. Sort by salary descending
sorted_employees = employees_df.sort_values(by='Salary', ascending=False)
print("\nEmployees Sorted by Salary (Desc):")
print(sorted_employees[['Name', 'Department', 'Salary']])


# Q20. Merge to show project allocations
import pandas as pd

# Load CSVs
employees_df = pd.read_csv('employees.csv')
projects_df = pd.read_csv('projects.csv')


merged_df = pd.merge(employees_df, projects_df, on='EmployeeID', how='inner')
print("\nMerged Dataset - Project Allocations:")
print(merged_df[['Name', 'ProjectName', 'HoursAllocated']])

# Q21. Employees NOT working on any project (left join + null project)
left_join_df = pd.merge(employees_df, projects_df, on='EmployeeID', how='left')
no_project_df = left_join_df[left_join_df['ProjectID'].isna()]
print("\nEmployees Not Assigned to Any Project:")
print(no_project_df[['EmployeeID', 'Name', 'Department']])

# Q22. Add derived column: TotalCost = HoursAllocated * (Salary / 160)
# Drop rows without HoursAllocated and create a copy to avoid warning
valid_projects_df = left_join_df.dropna(subset=['HoursAllocated']).copy()

# Compute TotalCost safely
valid_projects_df['TotalCost'] = valid_projects_df['HoursAllocated'] * (valid_projects_df['Salary'] / 160)

print("\nProject Cost Estimation:")
print(valid_projects_df[['Name', 'ProjectName', 'HoursAllocated', 'TotalCost']])"""











