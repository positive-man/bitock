
with open(r'C:\Users\wjjo\Documents\new 6.txt') as f:
    lines = f.read().split('\n')
    lines = [float(line) for line in lines]
    lines = [line for line in lines if line != -1]
    print(sum(lines))