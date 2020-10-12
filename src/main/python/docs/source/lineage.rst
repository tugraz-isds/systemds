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


Lineage Trace API
=================

Lineage tracing captures every instruction during runtime and store those in a DAG. 
An user can retreive the string representation of a lineage trace by enabling 
this feature. Each line of a lineage trace, which is called *lineage item*, 
represents a single operation. 
A *lineage item* is composed of an *id*, which is uniquely identifiable 
through an execution, an *opcode* for identifying the physical operation, 
a *name* to find a lineage item from an operand of the currently 
captured instruction, and a *data* attribute, which represents all necessary 
information. It also stores references to other *lineage items* 
which are used as inputs in the *inputs* array. 


Lineage items are categorized into three types.

**Literal(L):** This type of lineage items represent operands and are always 
leaf nodes. 
Literals provide information about their current value, 
the data type (e.g., tensor, matrix, or scalar), and the value type 
(e.g., double-precision floating-point, integer, boolean, or string). ::

  (8) (L) 2·SCALAR·INT64·true

**Creation (C):** This type represents data generation instructions 
such as ``rand``, ``read``, ``sample`` and includes only leaf nodes. 
It consists of a newly created variable name, the instruction and metadata. ::

  (52) (C) CP°rand°_Var30·SCALAR·INT64·false°1·SCALAR·INT64·true°1000°0°0°1.0°-1°uniform°1.0°1°_mVar31·MATRIX·FP64

**Instruction (I):** The last lineage item type is for computational 
instructions. This includes the *opcode*, and all the *input references*. ::

  (40) (I) + (12) (14)

.. This means that they return an ``OperationNode``.

Tracing
-------
Lineage can be traced in two ways:

1. By passing ``lineage = True`` as an argument to ``compute()``, e.g. ::

     model, trace = features.l2svm(labels).compute(lineage = True)

2. By calling ``.getLineageTrace()`` on any intermediate. e.g. ::

     trace = features.l2svm(labels).getLineageTrace()

.. note::

  Lineage trace is captured during execution of an instruction in SystemDS.
  That means, tracing lineage for an intermediate requires SystemDS to lazily
  execute the DAG of operations till the desired node.
  
The example below traces lineage of an DAG of operations.
Note the reorder of multiplication and sum operations::

  # Import full
  from systemds.matrix import full
  # Full generates a matrix completely filled with one number.
  # Generate a 5x10 matrix filled with 4.2
  m = full((5, 10), 4.20)
  # multiply with scala. Nothing is executed yet!
  m_res = m * 3.1
  # aggregate all the elements, compute the result 
  # and capture lineage trace as well.
  sum, lt = m_res.sum().compute(lineage = True)
  # print the lineage trace
  print(lt)

Output::

  (2) (L) 3.1·SCALAR·FP64·true
  (0) (C) CP°rand°5·SCALAR·INT64·true°10·SCALAR·INT64·true°1000°4.2°4.2°1.0°-1°uniform°1.0°1°_mVar0·MATRIX·FP64
  (1) (I) uak+ (0)
  (3) (I) * (2) (1)


:Author: Arnab Phani 
:Version: 1.0 of 26/03/2020
