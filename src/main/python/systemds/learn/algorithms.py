# -------------------------------------------------------------
#
# Copyright 2019 Graz University of Technology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# -------------------------------------------------------------
__all__ = ["l2svm"]

import sys
import os

try:
    import py4j.java_gateway
    from py4j.java_gateway import JavaObject
    from pyspark import SparkContext
    from pyspark.conf import SparkConf
    import pyspark.mllib.common
    from pyspark.sql import SparkSession
except ImportError:
    raise ImportError(
        'Unable to import `pyspark`. Hint: Make sure you are running with PySpark.')
path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
sys.path.insert(0, path)

from .. import *

ml_context = None


def getOrCreateMLContext():
    global ml_context
    if ml_context is None:
        ml_context = MLContext(SparkContext.getOrCreate())
    return ml_context


def l2svm(X, Y, intercept=False, epsilon=0.001, lmbda=1.0, max_iter=100):
    """
    Executes l2svm on the given data with the given parameters. Returns a `MLContext.Matrix` object.
    """
    script = dml("model = l2svm(X=X, Y=Y, intercept=i, epsilon=eps, lambda=l, maxiterations=maxi)") \
        .output("model").input(X=X, Y=Y, i=intercept, eps=epsilon, l=lmbda, maxi=max_iter)
    return getOrCreateMLContext().execute(script).get("model")
