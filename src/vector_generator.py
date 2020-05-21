import numpy as np
from numpy import asarray
from numpy import savetxt
data = np.random.uniform(-5.0, 5.0, (5000,2))
# save to csv file
savetxt('data.csv', data, delimiter='\t')
dot = np.dot(data[:,0], data[:,1])
print(dot)
