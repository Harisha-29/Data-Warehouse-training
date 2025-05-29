class Book:
    def __init__(self, title, author, price, in_stock):
        self.title = title
        self.author = author
        self.price = price
        self.in_stock = in_stock

    def sell(self, quantity):
        if quantity <= self.in_stock:
            self.in_stock -= quantity
            print(f"{quantity} copies of '{self.title}' sold. Remaining stock: {self.in_stock}")
        else:
            raise ValueError("Not enough stock to complete the sale.")

# User input
title = input("Enter book title: ")
author = input("Enter book author: ")
price = float(input("Enter book price: "))
stock = int(input("Enter number of copies in stock: "))
book = Book(title, author, price, stock)

try:
    qty = int(input("Enter quantity to sell: "))
    book.sell(qty)
except ValueError as e:
    print("Error:", e)
