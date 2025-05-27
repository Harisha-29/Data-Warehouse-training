# Palindrome Checker
text = input("Enter a string: ")

if text == text[::-1]:
    print("It is a palindrome.")
else:
    print("It is NOT a palindrome.")
