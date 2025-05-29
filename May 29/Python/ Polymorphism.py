class Animal:
    def speak(self):
        pass

class Dog(Animal):
    def speak(self):
        return "Woof!"

class Cat(Animal):
    def speak(self):
        return "Meow!"

class Cow(Animal):
    def speak(self):
        return "Moo!"

# User input
animals = []
count = int(input("How many animals? "))

for _ in range(count):
    animal_type = input("Enter animal type (dog/cat/cow): ").lower()
    if animal_type == "dog":
        animals.append(Dog())
    elif animal_type == "cat":
        animals.append(Cat())
    elif animal_type == "cow":
        animals.append(Cow())
    else:
        print("Unknown animal. Skipping.")

print("\nAnimal Sounds:")
for a in animals:
    print(a.speak())
