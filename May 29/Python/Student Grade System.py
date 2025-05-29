class Student:
    def __init__(self, name, marks):
        self.name = name
        self.marks = marks

    def average(self):
        return sum(self.marks) / len(self.marks)

    def grade(self):
        avg = self.average()
        if avg >= 90:
            return 'A'
        elif avg >= 75:
            return 'B'
        elif avg >= 50:
            return 'C'
        else:
            return 'F'

# User input
name = input("Enter student name: ")
num_subjects = int(input("Enter number of subjects: "))
marks = []

for i in range(num_subjects):
    mark = float(input(f"Enter marks for subject {i+1}: "))
    marks.append(mark)

student = Student(name, marks)
print(f"\nAverage Marks: {student.average():.2f}")
print("Grade:", student.grade())
