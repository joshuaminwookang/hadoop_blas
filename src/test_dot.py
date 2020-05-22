import numpy as np
from numpy import asarray
from numpy import savetxt
from numpy import genfromtxt
data = genfromtxt('data.csv', delimiter='\t')
dot = np.dot(data[:,0], data[:,1])
savetxt('test_out.csv', dot, delimiter='\t')     
