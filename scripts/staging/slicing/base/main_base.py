from init import init
from node import Node
from top_k import Topk


def opt_fun(fi, si, f, x_size):
    # weight of error function criteria
    w = 0.7
    formula = w * (fi/f) + (1 - w) * (si/x_size)
    return float(formula)


def check_for_nonsense(node_i, node_j, cur_lvl):
    commons = 0
    nonsense = False
    for attr1 in node_i.attributes:
        for attr2 in node_j.attributes:
            if attr1 == attr2:
                commons = commons + 1
    if commons != cur_lvl - 1:
        nonsense = True
    return nonsense


def Union(lst1, lst2):
    final_list = sorted(set(list(lst1) + list(lst2)))
    return final_list


init = init()
enc = init[0]
model = init[1]
complete_x = init[2]
complete_y = init[3]
f_l2 = init[4]
x_size = init[5]
# forming pairs of encoded features
all_features = enc.get_feature_names()
features_indexes = []
counter = 0
# First level slices are enumerated in a "classic way" (getting data and not analyzing bounds
first_level = []
levels = []
all_nodes = {}
cur_lvl = 0
for feature in all_features:
    features_indexes.append((feature, counter))
    new_node = Node()
    new_node.parents = [(feature, counter)]
    new_node.attributes.append((feature, counter))
    new_node.name = new_node.make_name()
    new_id = len(all_nodes)
    new_node.key = new_node.make_key(new_id)
    all_nodes[new_node.key] = new_node
    subset = new_node.make_first_level_slice()
    new_node.size = len(subset)
    fi_l2 = 0
    tuple_errors = []
    for j in range(0, len(subset)):
        fi_l2_sample = (model.predict(subset[j][1].reshape(1, -1)) -
                        complete_y[subset[j][0]][1]) ** 2
        tuple_errors.append(fi_l2_sample)
        fi_l2 = fi_l2 + fi_l2_sample
    new_node.l2_error = fi_l2 / new_node.size
    new_node.score = opt_fun(new_node.l2_error, new_node.size, f_l2, x_size)
    new_node.e_upper = max(tuple_errors)
    new_node.c_upper = new_node.score
    first_level.append(new_node)
    counter = counter + 1
levels.append(first_level)
alpha = 4  # size significance coefficient
candidates = []
for slice in first_level:
    if slice.score > 1 and slice.size >= x_size / alpha:
        candidates.append(slice)
# cur_lvl - index of current level, correlates with number of slice forming features
cur_lvl = 1  # currently filled level
k = 10  # number of top-slices we want
top_k = Topk(k)
n = len(candidates)
for candidate in candidates:
    top_k.add_new_top_slice(candidate)

while cur_lvl < len(all_features):
    min_top_score = top_k.min_score
    cur_lvl_nodes = []
    for node_i in range(len(levels[cur_lvl - 1]) - 1):
        for node_j in range(node_i + 1, len(levels[cur_lvl - 1]) - 1):
            flag = check_for_nonsense(levels[cur_lvl - 1][node_i], levels[cur_lvl - 1][node_j], cur_lvl)
            if not flag:
                new_node = Node()
                parents_set = set(new_node.parents)
                parents_set.add(levels[cur_lvl - 1][node_i])
                parents_set.add(levels[cur_lvl - 1][node_j])
                new_node.parents = parents_set
                parent1_attr = levels[cur_lvl - 1][node_i].attributes
                parent2_attr = levels[cur_lvl - 1][node_j].attributes
                new_node_attr = Union(parent1_attr, parent2_attr)
                new_node.attributes = new_node_attr
                new_node.name = new_node.make_name()
                new_id = len(all_nodes)
                new_node.key = new_node.make_key(new_id)
                if new_node.key in all_nodes:
                    existing_item = all_nodes[new_node.key]
                    parents_set = set(existing_item.parents)
                    parents_set.add(node_i)
                    parents_set.add(node_j)
                    existing_item.parents = parents_set
                else:
                    new_node.s_upper = new_node.calc_s_upper(cur_lvl)
                    new_node.s_lower = new_node.calc_s_lower(cur_lvl)
                    new_node.e_upper = new_node.calc_e_upper()
                    new_node.c_upper = new_node.calc_c_upper()
                    all_nodes[new_node.key] = new_node
                    if new_node.c_upper >= top_k.min_score:
                        subset = new_node.make_slice()
                        new_node.size = len(subset)
                        if new_node.size >= x_size / alpha:
                            new_node.l2_error = new_node.calc_l2_error(subset)
                            new_node.score = opt_fun(new_node.l2_error, new_node.size, f_l2, x_size)
                            top_k.add_new_top_slice(new_node)
                cur_lvl_nodes.append(new_node)
    cur_lvl = cur_lvl + 1
    levels.append(cur_lvl_nodes)

for candidate in top_k.slices:
    print(candidate.name + ": " + str(candidate.score))
