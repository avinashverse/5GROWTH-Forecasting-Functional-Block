import numpy as np


arr = [[-5.29328, 9.89139, -2.79590, -8.73531],  #Time=1 |
  [-5.29345, 9.89152, -2.79595, -8.73542],  #Time=2 |
  [-5.29396, 9.89200, -2.79644, -8.73593],  #Time=3 |
  [-5.29416, 9.89222, -2.79671, -8.73614],  #Time=4 V
  [-5.29345, 9.89152, -2.79595, -8.73542],  #Time=2 |
  [-5.29396, 9.89200, -2.79644, -8.73593],  #Time=3 |
  [-5.29416, 9.89222, -2.79671, -8.73614],  #Time=4 |
  [-5.29451, 9.89257, -2.79702, -8.73649],  #Time=5 V
  [-5.29396, 9.89200, -2.79644, -8.73593],  #Time=3 |
  [-5.29416, 9.89222, -2.79671, -8.73614],  #Time=4 |
  [-5.29451, 9.89257, -2.79702, -8.73649],  #Time=5 |
  [-5.29479, 9.89258, -2.79732, -8.73643]] #Time=6 V


arr1 = [[0, 0, 0, 0],  #Time=1 |
  [1, 1, 1, 1],  #Time=2 |
  [2, 2, 2, 2],  #Time=3 |
  [3, 3, 3, 3],  #Time=4 V
  [4, 4, 4, 4],  #Time=2 |
  [5, 5, 5, 5],  #Time=3 |
  [6, 6, 6, 6],  #Time=4 |
  [7, 7, 7, 7],  #Time=5 V
  [8, 8, 8, 8],  #Time=3 |
  [9, 9, 9, 9]] #Time=6 V


def extract_windows(array, starting_index, max_time, sub_window_size):
    examples = []
    start = starting_index + 1 - sub_window_size + 1
    print(start)
    for i in range(max_time + 1):
        example = array[start + i:start + sub_window_size + i]
        print( example)
        examples.append(np.expand_dims(example, 0))

    return np.vstack(examples)


#print(arr)
#aa = extract_windows(arr, 2, 6, 3)
#for i in range(0, len(aa)):
#    print(f'element {str(i)}: {aa[i]}\n')
#print(aa)


'''
x = np.arange(48.0).reshape(12, 4)
xx = np.arange(10.0).reshape(10, 1)
print(xx)
print(x)
y = np.hsplit(x, 4)
print(y)
z = y[0]
print(z.T)
'''

x1 = np.arange(20.0).reshape(10, 2)

x2 = np.arange(20.0, 32.0).reshape(6, 2)

print(len(x2))
print(x1)
print(x2)
if len(x2) == 10:
    x1 = x2
elif len(x2) > 10:
    x1 = x2[-10:]
else:
    init = 10-len(x2)
    temp = x1[-init:]
    print(temp)
    x1 = np.concatenate((temp, x2), axis=0)


print(x1)

print(len(x1))

a = np.append(x1, [[10., 11.]], axis=0)
print(a)

a1 = np.array([[0, 0]])


print(a1)
print(np.append(a, a1, axis=0))