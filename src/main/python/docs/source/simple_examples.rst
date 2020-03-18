.. ------------------------------------------------------------------------------
..  Copyright 2020 Graz University of Technology
..
..  Licensed under the Apache License, Version 2.0 (the "License");
..  you may not use this file except in compliance with the License.
..  You may obtain a copy of the License at
..
..    http://www.apache.org/licenses/LICENSE-2.0
..
..  Unless required by applicable law or agreed to in writing, software
..  distributed under the License is distributed on an "AS IS" BASIS,
..  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
..  See the License for the specific language governing permissions and
..  limitations under the License.
.. ------------------------------------------------------------------------------

Quickstart
==========

Let's take a look at some code examples.

Matrix Operations
-----------------

Making use of SystemDS, let us multiply an Matrix with an scala::

  >>> from systemds.matrix import full  # import full
  # full generates a matrix completely filled with one number
  >>> m = full((5, 10), 4.20)  # generate a 5x10 matrix filled with 4.2
  >>> m_res = m * 3.1  # multiply with scala. Nothing is executed yet!
  # Do the calculation in SystemDS and print the result (numpy array)
  >>> print(m_res.compute())

As output we get::

  [[ 13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02]
   [ 13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02]
   [ 13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02]
   [ 13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02]
   [ 13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02  13.02]]

The Python SystemDS package is compatible with numpy arrays.
Let us do a quick elementwise matrix multiplication of numpy arrays with SystemDS::

  >>> import numpy as np  # import numpy
  >>> from systemds.matrix import Matrix  # import Matrix class

  # create a random array
  >>> m1 = np.array(np.random.randint(100, size=5 * 5) + 1.01, dtype=np.double)
  >>> m1.shape = (5, 5)
  # create another random array
  >>> m2 = np.array(np.random.randint(5, size=5 * 5) + 1, dtype=np.double)
  >>> m2.shape = (5, 5)

  # elementwise matrix multiplication, note that nothing is executed yet!
  >>> m_res = Matrix(m1) * Matrix(m2)
  # lets do the actual computation in SystemDS! We get an numpy array as a result
  >>> m_res_np = m_res.compute()
  >>> print(m_res_np)

More complex operations
-----------------------

SystemDS provides high level functions for Data-Scientists, lets take a look at l2svm::

    >>> import numpy as np  # import numpy
    >>> from systemds.matrix import Matrix  # import Matrix class
    >>> np.random.seed(0)
    # generate random features in numpy
    >>> features = np.array(np.random.randint(100, size=10 * 10) + 1.01,
            dtype=np.double)
    >>> m1.shape = (10, 10)
    # generate random labels in numpy
    >>> labels = np.zeros((10, 1))
    # l2svm labels can only be 0 or 1
    >>> for i in range(10):
    >>>     if np.random.random() > 0.5:
    >>>         labels[i][0] = 1
    # compute our model
    >>> model = features.l2svm(labels).compute()

The output should be similar to::

  [[ 0.02033445]
   [-0.00324092]
   [ 0.0014692 ]
   [ 0.02649209]
   [-0.00616902]
   [-0.0095087 ]
   [ 0.01039221]
   [-0.0011352 ]
   [-0.01686351]
   [-0.03839821]]
