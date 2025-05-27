# Triangle Validator
a = float(input("Enter side a: "))
b = float(input("Enter side b: "))
c = float(input("Enter side c: "))

if (a + b > c) and (a + c > b) and (b + c > a):
    print("These sides can form a valid triangle.")
else:
    print("These sides CANNOT form a valid triangle.")
