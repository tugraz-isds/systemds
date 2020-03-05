#-------------------------------------------------------------
#
# Copyright 2020 Graz University of Technology
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
#-------------------------------------------------------------

import pandas as pd
from sklearn.preprocessing import OneHotEncoder

import slicing.slicer as slicer
from sklearn.ensemble import RandomForestClassifier
from sklearn import preprocessing
from sklearn.model_selection import train_test_split


dataset = pd.read_csv('adult.csv')
attributes_amount = len(dataset.values[0])
x = dataset.iloc[:, 0:attributes_amount - 1].values
# enc = OneHotEncoder(handle_unknown='ignore')
# x = enc.fit_transform(x).toarray()
y = dataset.iloc[:, attributes_amount - 1]
le = preprocessing.LabelEncoder()
le.fit(y)
y = le.transform(y)
complete_x = []
complete_y = []
counter = 0
all_indexes = []
not_encoded_columns = [
    "Age", "WorkClass", "fnlwgt", "Education", "EducationNum",
    "MaritalStatus", "Occupation", "Relationship", "Race", "Gender",
    "CapitalGain", "CapitalLoss", "HoursPerWeek", "NativeCountry", "Income"
]
for row in x:
        row[0] = int(row[0] / 10)
        row[2] = int(row[2]) // 100000
        row[4] = int(row[4] / 5)
        row[10] = int(row[10] / 1000)
        row[12] = int(row[12] / 10)
enc = OneHotEncoder(handle_unknown='ignore')
x = enc.fit_transform(x).toarray()
all_features = enc.get_feature_names()
x_size = len(complete_x)
x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)
for item in x_test:
    complete_x.append((counter, item))
    complete_y.append((counter, y_test[counter]))
    counter = counter + 1
x_size = counter
clf = RandomForestClassifier(n_jobs=2, random_state=0)
clf.fit(x_train, y_train)
RandomForestClassifier(bootstrap=True, class_weight=None, criterion='gini',
            max_depth=None, max_features='auto', max_leaf_nodes=None,
            min_impurity_split=1e-07, min_samples_leaf=1,
            min_samples_split=2, min_weight_fraction_leaf=0.0,
            n_estimators=10, n_jobs=2, oob_score=False, random_state=0,
            verbose=0, warm_start=False)

# alpha is size significance coefficient
# verbose option is for returning debug info while creating slices and printing it
# k is number of top-slices we want
# w is a weight of error function significance (1 - w) is a size significance propagated into optimization function
# loss_type = 0 (l2 in case of regression model
# loss_type = 1 (cross entropy in case of classification model)
preds = clf.predict(x_test)
predictions = []
counter = 0
mistakes = 0
for pred in preds:
    predictions.append((counter, pred))
    if y_test[counter] != pred:
        mistakes = mistakes + 1
    counter = counter + 1
lossF = mistakes / counter

# enumerator <union>/<join> indicates an approach of next level slices combination process:
# in case of <join> in order to create new node of current level slicer
# combines only nodes of previous layer with each other
# <union> case implementation is based on DPSize algorithm
enumerator = "union"
if enumerator == "join":
    slicer.process(all_features, clf, complete_x, lossF, x_size, complete_y, predictions, debug=True, alpha=4, k=10,
                   w=0.5, loss_type=1)
elif enumerator == "union":
    union_slicer.process(all_features, clf, complete_x, lossF, x_size, complete_y, predictions, debug=True, alpha=4,
                         k=10, w=0.5, loss_type=1)
