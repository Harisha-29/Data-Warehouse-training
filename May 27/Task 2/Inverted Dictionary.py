# Input dictionary
n = int(input("How many key-value pairs? "))
original = {}

for _ in range(n):
    key = input("Enter key: ")
    value = input("Enter value: ")
    original[key] = value
inverted = {v: k for k, v in original.items()}
print("Inverted dictionary:", inverted)
