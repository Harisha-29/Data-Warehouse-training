nums = [5, 12, 8, 23, 7, 23]
first = second = float('-inf')

for num in nums:
    if num > first:
        second = first
        first = num
    elif first > num > second:
        second = num

if second == float('-inf'):
    print("No second largest number found.")
else:
    print("Second largest number is:", second)
