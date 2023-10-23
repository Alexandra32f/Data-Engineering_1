filename = 'text_2_var_15'

with open(filename) as file:
    lines = file.readlines()

mean_lines = list()

for line in lines:
    nums = line.split('/')
    mean_line = 0
    for num in nums:
        mean_line += int(num)

    n = len(nums)
    m = mean_line / n
    #print(m)

    mean_lines.append(m)

#print(mean_lines)

with open('r_text_2.txt', 'w') as result:
    for value in mean_lines:
        result.write(str(value) + '\n')