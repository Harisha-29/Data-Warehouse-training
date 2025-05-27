# Double even numbers only
nums = [1, 2, 3, 4, 5, 6]
compressed = [x * 2 for x in nums if x % 2 == 0]
print("Compressed list (even numbers doubled):", compressed)
