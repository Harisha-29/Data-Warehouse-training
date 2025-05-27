# Get two tuples from user and swap
t1 = tuple(input("Enter elements of first tuple separated by space: ").split())
t2 = tuple(input("Enter elements of second tuple separated by space: ").split())

# Swap
t1, t2 = t2, t1

print("After swapping:")
print("Tuple 1:", t1)
print("Tuple 2:", t2)
