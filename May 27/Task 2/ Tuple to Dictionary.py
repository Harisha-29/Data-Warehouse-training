# Convert user input of key-value pairs to a dictionary
n = int(input("How many key-value pairs? "))
pairs = []

for i in range(n):
    key = input(f"Enter key {i+1}: ")
    value = input(f"Enter value for key '{key}': ")
    pairs.append((key, value))

tuple_data = tuple(pairs)
converted_dict = dict(tuple_data)

print("Converted dictionary:", converted_dict)
