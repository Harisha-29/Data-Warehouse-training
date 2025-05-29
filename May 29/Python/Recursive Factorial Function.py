def factorial(n):
    if n < 0:
        return "Factorial not defined for negative numbers"
    elif n == 0 or n == 1:
        return 1
    else:
        return n * factorial(n - 1)

# Example usage:
print("\nFactorial of 5:", factorial(5))
print("Factorial of 0:", factorial(0))
