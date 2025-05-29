class BankAccount:
    def __init__(self, name, balance=0):
        self.name = name
        self.balance = balance

    def deposit(self, amount):
        if amount > 0:
            self.balance += amount
            print(f"{amount} deposited. New balance: {self.balance}")
        else:
            print("Deposit amount must be positive.")

    def withdraw(self, amount):
        if amount <= self.balance:
            self.balance -= amount
            print(f"{amount} withdrawn. New balance: {self.balance}")
        else:
            print("Insufficient balance.")

    def get_balance(self):
        return self.balance

# User input
name = input("Enter account holder's name: ")
initial_balance = float(input("Enter initial balance: "))
account = BankAccount(name, initial_balance)

while True:
    print("\n1. Deposit\n2. Withdraw\n3. Check Balance\n4. Exit")
    choice = input("Enter your choice: ")

    if choice == '1':
        amt = float(input("Enter amount to deposit: "))
        account.deposit(amt)
    elif choice == '2':
        amt = float(input("Enter amount to withdraw: "))
        account.withdraw(amt)
    elif choice == '3':
        print("Current Balance:", account.get_balance())
    elif choice == '4':
        break
    else:
        print("Invalid choice.")
