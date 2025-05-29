class Rectangle:
    def __init__(self, length, width):
        self.length = length
        self.width = width

    def area(self):
        return self.length * self.width

    def perimeter(self):
        return 2 * (self.length + self.width)

    def is_square(self):
        return self.length == self.width

# User input
length = float(input("Enter length of the rectangle: "))
width = float(input("Enter width of the rectangle: "))
rect = Rectangle(length, width)

print("Area:", rect.area())
print("Perimeter:", rect.perimeter())
print("Is square?", rect.is_square())

