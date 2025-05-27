words = input("Enter words separated by space: ").split()
length_dict = {}

for word in words:
    length = len(word)
    if length not in length_dict:
        length_dict[length] = []
    length_dict[length].append(word)

print("Words grouped by length:")
for length, word_list in length_dict.items():
    print(f"{length}: {word_list}")
