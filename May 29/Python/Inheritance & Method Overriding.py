class Vehicle:
    def __init__(self, name, wheels):
        self.name = name
        self.wheels = wheels

    def description(self):
        print(f"{self.name} has {self.wheels} wheels.")

class Car(Vehicle):
    def __init__(self, name, wheels, fuel_type):
        super().__init__(name, wheels)
        self.fuel_type = fuel_type

    def description(self):
        print(f"{self.name} is a car with {self.wheels} wheels and runs on {self.fuel_type}.")

class Bike(Vehicle):
    def __init__(self, name, wheels, is_geared):
        super().__init__(name, wheels)
        self.is_geared = is_geared

    def description(self):
        geared_str = "geared" if self.is_geared else "non-geared"
        print(f"{self.name} is a bike with {self.wheels} wheels and is {geared_str}.")

# User input
vtype = input("Enter vehicle type (car/bike): ").lower()
name = input("Enter vehicle name: ")
wheels = int(input("Enter number of wheels: "))

if vtype == "car":
    fuel = input("Enter fuel type (Petrol/Diesel/Electric): ")
    vehicle = Car(name, wheels, fuel)
elif vtype == "bike":
    geared = input("Is the bike geared? (yes/no): ").lower() == 'yes'
    vehicle = Bike(name, wheels, geared)
else:
    print("Invalid vehicle type.")
    vehicle = None

if vehicle:
    vehicle.description()
