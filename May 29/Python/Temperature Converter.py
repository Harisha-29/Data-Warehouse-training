def convert_temp(value, unit):
    if unit.lower() == 'c':
        return (value * 9/5) + 32  # Celsius to Fahrenheit
    elif unit.lower() == 'f':
        return (value - 32) * 5/9  # Fahrenheit to Celsius
    else:
        return "Invalid unit. Use 'C' for Celsius or 'F' for Fahrenheit."
print("\n\nTemperature Conversion Examples:")
print("25°C to Fahrenheit:", convert_temp(25, 'C'))
print("77°F to Celsius:", convert_temp(77, 'F'))
