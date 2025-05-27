import random

# Generate random number between 1 and 100
secret_number = random.randint(1, 100)
guess = None

print("Welcome to the Number Guessing Game!")
print("I'm thinking of a number between 1 and 100.")

# Loop until correct guess
while guess != secret_number:
    try:
        guess = int(input("Enter your guess: "))
        if guess < secret_number:
            print("Too Low! Try again.")
        elif guess > secret_number:
            print("Too High! Try again.")
        else:
            print("Congratulations! You guessed it right.")
    except ValueError:
        print("Invalid input. Please enter an integer.")
