# Get student details from user
data = input("Enter student details (name age branch grade) separated by space: ").split()
if len(data) == 4:
    name, age, branch, grade = data
    print(f"Student {name} is {age} years old, studies {branch}, and has a grade of {grade}.")
else:
    print("Please enter exactly 4 values.")
