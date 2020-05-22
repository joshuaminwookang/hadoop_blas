import numpy as np
from numpy import asarray
from numpy import savetxt

n = 1000
ma = np.random.uniform(-5.0, 5.0, (n,n))
mboriginal = np.random.uniform(-5.0, 5.0, (n,n))
mb = mboriginal.transpose()

newB = mb
for i in range(n-1):
    newB = np.concatenate((newB,mb),axis=0)
data=np.concatenate((np.repeat(ma, n, axis=0), newB),axis=1)

# save to csv file
savetxt('input_matrix.csv', data, delimiter='\t')
savetxt('ma.csv', ma, delimiter='\t')
savetxt('mb.csv', mboriginal, delimiter='\t')

