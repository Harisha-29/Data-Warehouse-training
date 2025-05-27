# Get two sets of integers
set1 = set(map(int, input("Enter integers for Set 1 (space-separated): ").split()))
set2 = set(map(int, input("Enter integers for Set 2 (space-separated): ").split()))

sym_diff = set1.symmetric_difference(set2)
print("Symmetric difference:", sym_diff)
