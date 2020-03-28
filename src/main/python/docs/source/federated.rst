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


Federated SystemDS
==================

The python SystemDS supports federated execution.
To enable this, each of the federated environments have to have 
a running federated worker.

Start Federated worker
----------------------

To start a federated worker, you first have to setup your environment variables.
A simple guide to do this is in the SystemDS Repository_.

.. _Repository: https://github.com/apache/systemml/tree/master/bin/

If that is setup correctly simply start a worker using the following command.

  systemds.sh WORKER 8001

Simple Aggregation Example
--------------------------

In this example we use a single federated worker, and aggregate the sum of its data.

First we need to create some data for our federated worker to use.
In this example we simply use Numpy to create a ``test.csv`` file.::

  # Import numpy
  import numpy as np
  a = np.asarray([[1,2,3],[4,5,6],[7,8,9]])
  np.savetxt("temp/test.csv", a, delimiter=",")

Currently we also require a metadata file for the federated worker.
This should be located next to the ``test.csv`` file.
To make this simply execute the following::

  echo '{ "format":"csv", "header":false, "rows":3, "cols":3 }' > temp/test.csv.mtd

After creating our data we start a federated worker instance the 
federated instructions can be executed. 
The multiply using federated instructions in python SystemDS is done
as follows::

  # Import numpy and SystemDS federated
  import numpy as np
  from systemds.matrix import federated
  # Create a federated matrix
  fed_a = federated(["localhost:8001/temp/test.csv"],[([0,0],[3,3])])
  # Sum the federated matrix and call compute to execute
  print(fed_a.sum().compute())
  # Result should be 45.

Multiple Federated Environments 
-------------------------------

In this example we multiply matrices that are located in different federated environments.
Using the data created from the last example we can simulate
multiple federated workers by simply starting multiple ones on different ports.
I recommend to start 3 different terminals, and run one federated environment in each.

| systemds.sh WORKER 8001
| systemds.sh WORKER 8002
| systemds.sh WORKER 8003

Then the code would look as follows::

  # Import numpy and SystemDS federated
  import numpy as np
  from systemds.matrix import federated
  # Create a federated matrix using two federated environments
  # Note that the two federated matrices are stacked on top of each other
  fed_a = federated([
	  "localhost:8001/temp/test.csv",
	  "localhost:8002/temp/test.csv"
	  ],[([0,0],[3,3]),([3,3],[3,3])])
  # Create another federated matrix using the last environment.
  fed_b = federated(["localhost:8003/temp/test.csv"],[([0,0],[3,3])])
  # Multiply, compute and print.
  res = (fed_a * fed_b).compute()
  print(res)
  # Result should be 135.


:Author: Sebastian Baunsgaard
:Version: 1.0 of 2020/03/26
