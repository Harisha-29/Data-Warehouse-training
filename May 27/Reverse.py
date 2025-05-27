
num = int(input("Enter a 3-digit number: "))

if 100 <= num <= 999:
    reversed_num = int(str(num)[::-1])
    print(f"Reversed number: {reversed_num}")
else:
    print("Please enter a valid 3-digit number.")
