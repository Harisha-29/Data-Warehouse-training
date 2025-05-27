students = {}
for i in range(3):
    name = input(f"Enter name of student {i+1}: ")
    marks = int(input(f"Enter marks for {name}: "))
    students[name] = marks
search_name = input("Enter student name to get grade: ")

if search_name in students:
    marks = students[search_name]
    if marks >= 90:
        grade = 'A'
    elif marks >= 75:
        grade = 'B'
    else:
        grade = 'C'
    print(f"{search_name}'s grade is: {grade}")
else:
    print("Student not found.")
