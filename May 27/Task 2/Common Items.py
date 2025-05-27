# Get two lists from user
list1 = input("Enter elements of first list (space-separated): ").split()
list2 = input("Enter elements of second list (space-separated): ").split()

# Convert to sets
set1 = set(list1)
set2 = set(list2)

# Find common elements
common = set1.intersection(set2)
print("Common elements:", common)
