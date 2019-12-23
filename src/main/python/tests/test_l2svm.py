# -------------------------------------------------------------
#
# Modifications Copyright 2019 Graz University of Technology
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# -------------------------------------------------------------

# To run:
#   - Python 2: `PYSPARK_PYTHON=python2 spark-submit --master local[*] --driver-class-path SystemDS.jar test_l2svm.py`
#   - Python 3: `PYSPARK_PYTHON=python3 spark-submit --master local[*] --driver-class-path SystemDS.jar test_l2svm.py`

# Make the `systemml` package importable
import os
import sys

path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
sys.path.insert(0, path)

import unittest

import numpy as np
from pyspark.context import SparkContext

from systemds import MLContext, dml, matrix, learn

sc = SparkContext.getOrCreate()
ml = MLContext(sc)


class TestAPI(unittest.TestCase):

    def test_10x10_manual(self):
        dim = 10
        np.random.seed(1304)
        m1 = np.array(np.random.randint(100, size=dim * dim) + 1.01, dtype=np.double)
        m1.shape = (dim, dim)
        m2 = np.zeros((dim, 1))
        for i in range(dim):
            if np.random.random() > 0.5:
                m2[i][0] = 1
        script = dml("model = l2svm(X=X, Y=Y)").output("model").input(X=m1, Y=m2)
        model = ml.execute(script).get("model").toNumPy()
        # TODO calculate reference
        self.assertTrue(np.allclose(model, np.array([[-0.03277166],
                                                     [-0.00820981],
                                                     [0.00657115],
                                                     [0.03228764],
                                                     [-0.01685067],
                                                     [0.00892918],
                                                     [0.00945636],
                                                     [0.01514383],
                                                     [0.0713272],
                                                     [-0.05113976]])))

    def test_10x10_func(self):
        dim = 10
        np.random.seed(1304)
        m1 = np.array(np.random.randint(100, size=dim * dim) + 1.01, dtype=np.double)
        m1.shape = (dim, dim)
        m2 = np.zeros((dim, 1))
        for i in range(dim):
            if np.random.random() > 0.5:
                m2[i][0] = 1
        sc = SparkContext.getOrCreate()
        model = learn.l2svm(m1, m2).toNumPy()
        # TODO calculate reference
        self.assertTrue(np.allclose(model, np.array([[-0.03277166],
                                                     [-0.00820981],
                                                     [0.00657115],
                                                     [0.03228764],
                                                     [-0.01685067],
                                                     [0.00892918],
                                                     [0.00945636],
                                                     [0.01514383],
                                                     [0.0713272],
                                                     [-0.05113976]])))


if __name__ == "__main__":
    unittest.main()
