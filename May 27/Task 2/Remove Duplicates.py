# Remove duplicates while preserving order
original = [1, 2, 2, 3, 1, 4, 5, 3]
unique = []

for item in original:
    if item not in unique:
        unique.append(item)

print("List without duplicates:", unique)
