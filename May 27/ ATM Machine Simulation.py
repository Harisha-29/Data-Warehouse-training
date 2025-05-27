# ATM Machine Simulation
balance = 10000  # initial balance


def display_menu():
    print("\nATM MENU")
    print("1. Deposit")
    print("2. Withdraw")
    print("3. Check Balance")
    print("4. Exit")


while True:
    display_menu()
    choice = input("Enter your choice (1-4): ")

    if choice == '1':
        amount = float(input("Enter amount to deposit: "))
        if amount > 0:
            balance += amount
            print(f"₹{amount:.2f} deposited successfully.")
        else:
            print("Invalid deposit amount.")

    elif choice == '2':
        amount = float(input("Enter amount to withdraw: "))
        if amount > balance:
            print("Insufficient balance.")
        elif amount <= 0:
            print("Invalid withdrawal amount.")
        else:
            balance -= amount
            print(f"₹{amount:.2f} withdrawn successfully.")

    elif choice == '3':
        print(f"Your current balance is: ₹{balance:.2f}")

    elif choice == '4':
        print("Thank you for using the ATM. Goodbye!")
        break

    else:
        print("Invalid choice. Please select from 1 to 4.")
