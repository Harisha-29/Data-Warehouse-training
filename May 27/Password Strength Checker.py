import string

password = input("Enter a password to check strength: ")

# Criteria checks
has_length = len(password) >= 8
has_number = any(char.isdigit() for char in password)
has_upper = any(char.isupper() for char in password)
has_symbol = any(char in string.punctuation for char in password)

# Result
if has_length and has_number and has_upper and has_symbol:
    print("Strong password ")
else:
    print("Weak password ❌. Make sure it has:")
    if not has_length:
        print("- At least 8 characters")
    if not has_number:
        print("- At least one number (0-9)")
    if not has_upper:
        print("- At least one uppercase letter (A-Z)")
    if not has_symbol:
        print("- At least one symbol (e.g., !, @, #, $)")
