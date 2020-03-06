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

class Node:
    error: float
    name: ""
    attributes: []
    parents: []
    children: []
    size: int
    loss: float
    score: float
    e_max: float
    s_upper: int
    s_lower: int
    e_upper: float
    c_upper: float
    e_max_upper: float
    key: ""

    def __init__(self, all_features, model, complete_x, loss, x_size, y_test, preds):
        self.error = loss,
        self.parents = []
        self.attributes = []
        self.size = 0
        self.score = 0
        self.model = model
        self.complete_x = complete_x
        self.loss = 0
        self.x_size = x_size
        self.preds = preds
        self.s_lower = 1
        self.y_test = y_test
        self.all_features = all_features
        self.key = ''

    def calc_c_upper(self, w):
        upper_score = w * (self.e_upper / self.s_lower) / (float(self.error[0]) / self.x_size) + (1 - w) * self.s_upper
        return float(upper_score)

    def make_slice_mask(self):
        mask = []
        for feature in self.attributes:
            mask.append(feature[1])
        return mask

    def process_slice(self, loss_type):
        mask = self.make_slice_mask()
        if loss_type == 0:
            self.calc_l2(mask)
        if loss_type == 1:
            self.calc_class(mask)

    def calc_class(self, mask):
        self.e_max = 1
        size = 0
        mistakes = 0
        for row in self.complete_x:
            flag = True
            for attr in mask:
                if row[1][attr] == 0:
                    flag = False
            if flag:
                size = size + 1
                if self.y_test[row[0]][1] != self.preds[row[0]][1]:
                    mistakes = mistakes + 1
        self.size = size
        if size != 0:
            self.loss = mistakes / size
        else:
            self.loss = 0
        self.e_upper = self.loss

    def calc_l2(self, mask):
        max_tuple_error = 0
        sum_error = 0
        size = 0
        for row in self.complete_x:
            flag = True
            for attr in mask:
                if row[1][attr] == 0:
                    flag = False
            if flag:
                size = size + 1
                if float(self.preds[row[0]][1]) > max_tuple_error:
                    max_tuple_error = float(self.preds[row[0]][1])
                sum_error = sum_error + float(self.preds[row[0]][1])
        self.e_max = max_tuple_error
        self.e_upper = max_tuple_error
        if size != 0:
            self.loss = sum_error/size
        else:
            self.loss = 0
        self.size = size

    def calc_s_upper(self, cur_lvl):
        cur_min = self.parents[0].size
        for parent in self.parents:
            if cur_lvl == 1:
                cur_min = min(cur_min, parent.size)
            else:
                cur_min = min(cur_min, parent.s_upper)
        return cur_min

    def calc_e_max_upper(self, cur_lvl):
        if cur_lvl == 1:
            e_max_min = self.parents[0].e_max
        else:
            e_max_min = self.parents[0].e_max_upper
        for parent in self.parents:
            if cur_lvl == 1:
                e_max_min = min(e_max_min, parent.e_max)
            else:
                e_max_min = min(e_max_min, parent.e_max_upper)
        return e_max_min

    def calc_s_lower(self, cur_lvl):
        size_value = self.x_size
        for parent in self.parents:
            size_value = size_value - (self.x_size - parent.s_lower)
        return max(size_value, 1)

    def calc_e_upper(self):
        prev_e_uppers = []
        for parent in self.parents:
            prev_e_uppers.append(parent.e_upper)
        return float(min(prev_e_uppers))

    def calc_bounds(self, cur_lvl, w):
        self.s_upper = self.calc_s_upper(cur_lvl)
        self.s_lower = self.calc_s_lower(cur_lvl)
        self.e_upper = self.calc_e_upper()
        self.e_max_upper = self.calc_e_max_upper(cur_lvl)
        self.c_upper = self.calc_c_upper(w)

    def make_name(self):
        name = ""
        for attribute in self.attributes:
            name = name + str(attribute[0]) + " && "
        name = name[0: len(name) - 4]
        return name

    def make_key(self, new_id):
        return new_id, self.name

    def check_constraint(self, top_k, x_size, alpha):
        return self.score >= top_k.min_score and self.size >= x_size / alpha

    def check_bounds(self, top_k, x_size, alpha):
        return self.s_upper >= x_size / alpha and self.c_upper >= top_k.min_score

    def print_debug(self, topk, level):
        print("new node has been created: " + self.make_name() + "\n")
        if level >= 1:
            print("s_upper = " + str(self.s_upper))
            print("s_lower = " + str(self.s_lower))
            print("e_upper = " + str(self.e_upper))
            print("c_upper = " + str(self.c_upper))
        print("size = " + str(self.size))
        print("score = " + str(self.score))
        print("current topk min score = " + str(topk.min_score))
        print("-------------------------------------------------------------------------------------")
