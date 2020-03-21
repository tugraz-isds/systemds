from typing import Tuple

import numpy as np

from systemds.matrix import Matrix



def getl2svm():
    features, labels = generate_matrices_for_l2svm(10, seed=1304)
    #get the lineage trace
    lt = features.l2svm(labels).getLineageTrace()
    return lt
	

def generate_matrices_for_l2svm(dims: int, seed: int = 1234) -> Tuple[Matrix, Matrix]:
    np.random.seed(seed)
    m1 = np.array(np.random.randint(100, size=dims * dims) + 1.01, dtype=np.double)
    m1.shape = (dims, dims)
    m2 = np.zeros((dims, 1))
    for i in range(dims):
        if np.random.random() > 0.5:
            m2[i][0] = 1
    return Matrix(m1), Matrix(m2)

print(getl2svm())



