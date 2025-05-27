# Input sentence from user
sentence = input("Enter a sentence: ")

# Split into words and convert to set
unique_words = set(sentence.split())
print("Unique words:", unique_words)
