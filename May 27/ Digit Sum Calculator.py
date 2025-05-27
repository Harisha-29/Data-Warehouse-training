
num = int(input("Enter a number: "))
sum_of_digits = 0
temp = num

while temp > 0:
    sum_of_digits += temp % 10
    temp //= 10

print(f"Sum of digits of {num} is {sum_of_digits}")
