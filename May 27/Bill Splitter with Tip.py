# Bill Splitter with Tip
bill = float(input("Enter total bill amount: "))
people = int(input("Enter number of people: "))
tip_percent = float(input("Enter tip percentage (e.g. 10 for 10%): "))

tip_amount = (tip_percent / 100) * bill
total_bill = bill + tip_amount
amount_per_person = total_bill / people

print(f"Total bill with tip: ₹{total_bill:.2f}")
print(f"Amount per person: ₹{amount_per_person:.2f}")
