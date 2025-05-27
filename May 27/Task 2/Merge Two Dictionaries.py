# First dictionary input
d1 = {}
n1 = int(input("How many key-value pairs in first dictionary? "))
for _ in range(n1):
    key = input("Enter key: ")
    value = int(input(f"Enter value for {key}: "))
    d1[key] = value

d2 = {}
n2 = int(input("How many key-value pairs in second dictionary? "))
for _ in range(n2):
    key = input("Enter key: ")
    value = int(input(f"Enter value for {key}: "))
    d2[key] = value
merged = d1.copy()
for key, value in d2.items():
    merged[key] = merged.get(key, 0) + value

print("Merged dictionary:", merged)
