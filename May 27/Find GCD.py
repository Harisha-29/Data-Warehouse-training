# GCD Finder using Euclidean Algorithm
a = int(input("Enter first number: "))
b = int(input("Enter second number: "))

# Ensure a >= b
while b != 0:
    a, b = b, a % b

print(f"The GCD is: {a}")
