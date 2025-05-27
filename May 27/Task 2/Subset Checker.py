
setA = set(input("Enter elements for Set A (space-separated): ").split())
setB = set(input("Enter elements for Set B (space-separated): ").split())

# Check if one is subset of the other
if setA.issubset(setB):
    print("Set A is a subset of Set B")
elif setB.issubset(setA):
    print("Set B is a subset of Set A")
else:
    print("Neither set is a subset of the other")
