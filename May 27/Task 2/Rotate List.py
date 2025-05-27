# Rotate list right by k steps
def rotate_right(lst, k):
    k %= len(lst)  # handle k > length
    return lst[-k:] + lst[:-k]

nums = [1, 2, 3, 4, 5]
k = 2
rotated = rotate_right(nums, k)
print(f"List after rotating right by {k} steps:", rotated)
