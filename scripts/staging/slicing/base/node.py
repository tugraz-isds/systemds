from init import init

init = init()
enc = init[0]
model = init[1]
complete_x = init[2]
complete_y = init[3]
f_l2 = init[4]
x_size = init[5]
x_test = init[6]
y_test = init[7]


class Node:
    name: ""
    attributes: []
    parents: []
    children: []
    size: int
    l2_error: float
    score: float
    s_upper: int
    s_lower: int
    e_upper: float
    c_upper: float

    def __init__(self):
        self.parents = []
        self.attributes = []

    def calc_c_upper(self):
        upper_score = (self.e_upper / self.s_lower) / (f_l2 / x_size) + self.s_upper
        return float(upper_score)

    def make_slice(self):
        subset = []
        for row in complete_x:
            flag = True
            for feature in self.attributes:
                if row[1][feature[1]] != 1:
                    flag = False
            if flag:
                subset.append(row)
        return subset

    def make_first_level_slice(self):
        subset = []
        for row in complete_x:
            flag = True
            for feature in self.attributes:
                if row[1][feature[1]] != 1:
                    flag = False
            if flag:
                subset.append(row)
        return subset

    def calc_s_upper(self, cur_lvl):
        sizes = []
        for parent in self.parents:
            if cur_lvl == 1:
                sizes.append(parent.size)
            else:
                sizes.append(parent.s_upper)
        return min(sizes)

    def calc_s_lower(self, cur_lvl):
        size_value = x_size
        for parent in self.parents:
            if cur_lvl == 1:
                size_value = size_value - (x_size - parent.size)
            else:
                size_value = size_value - (x_size - parent.s_lower)
        return max(size_value, 1)

    def calc_e_upper(self):
        prev_e_uppers = []
        for parent in self.parents:
            prev_e_uppers.append(parent.e_upper)
        return float(min(prev_e_uppers))

    def make_name(self):
        name = ""
        for attribute in self.attributes:
            name = name + str(attribute[0]) + " && "
        name = name[0: len(name) - 4]
        return name

    def make_key(self, new_id):
        return new_id, self.name

    def calc_l2_error(self, subset):
        fi_l2 = 0
        for i in range(0, len(subset)):
            fi_l2_sample = (model.predict(subset[i][1].reshape(1, -1)) - complete_y[subset[i][0]][1]) ** 2
            fi_l2 = fi_l2 + float(fi_l2_sample)
        fi_l2 = fi_l2 / len(subset)
        return float(fi_l2)







