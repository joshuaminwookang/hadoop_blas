import numpy as np
from numpy import asarray
from numpy import savetxt
from numpy import genfromtxt
ma = genfromtxt('ma.csv', delimiter='\t')
mb = genfromtxt('mb.csv', delimiter='\t')

savetxt('test_out.csv', np.matmul(ma,mb), delimiter='\t')     
