import numpy as np
import pandas as pd
import random

def add2dict(dictionary, key, value):
    if key not in dictionary:
        dictionary[key] = []
    dictionary[key].append(value)

def list2probabilitydict(given_list):
    probability_dict = {}
    given_list_length = len(given_list)
    for item in given_list:
        probability_dict[item] = probability_dict.get(item, 0) + 1
    for key, value in probability_dict.items():
        probability_dict[key] = value / given_list_length
    return probability_dict

def sample_word(dictionary):
    key = np.random.choice(list(dictionary.keys()), 1, p = list(dictionary.values()))[0]
    return key

def generate(sentence, no_of_words):
    sentence = sentence.split()
    if len(sentence) == 0:
        word0 = sample_word(initial_word)        
        sentence.append(word0)

    if len(sentence) == 1:
        word0 = sentence[0]
        if word0 in second_word.keys():
            word1 = sample_word(second_word[word0])
        else:
            word1 = np.random.choice(list(initial_word.keys()), 1, p = list(initial_word.values()))[0]
        sentence.append(word1)
        no_of_words = no_of_words - 1
    
    if len(sentence) == 2:
        word0 = sentence[-2]
        word1 = sentence[-1]
        if (word0, word1) in transitions.keys():
            word2 = sample_word(transitions[(word0, word1)])
        else:
            if word0 in second_word.keys():
                word2_tmp1 = sample_word(second_word[word0])
                dict1 = second_word[word2_tmp1]
                word2_tmp1 = sample_word(dict1)
            else:
                word2_tmp1 = np.random.choice(list(second_word.keys()), 1)
                dict1 = second_word[word2_tmp1[0]]
                word2_tmp1 = sample_word(dict1)
            if word1 in second_word.keys():
                word2_tmp2 = sample_word(second_word[word1])
                dict2 = second_word[word2_tmp2]
                word2_tmp2 = sample_word(dict2)
            else:
                word2_tmp2 = np.random.choice(list(second_word.keys()), 1)
                dict2 = second_word[word2_tmp2[0]]
                word2_tmp2 = sample_word(dict2)
            word2 = word2_tmp1 if dict1[word2_tmp1] > dict2[word2_tmp2] else word2_tmp2
        sentence.append(word2)
        no_of_words = no_of_words - 1
    
    while no_of_words > 0:
        if (word0, word1, word2) in transitions.keys():
            word3 = sample_word(transitions[(word0, word1, word2)])
        else:
            if (word0, word1) in transitions.keys():
                word3 = sample_word(transitions[(word0, word1)])
            elif (word0, word2) in transitions.keys():
                word3 = sample_word(transitions[(word0, word2)])
            elif (word1, word2) in transitions.keys():
                word3 = sample_word(transitions[(word1, word2)])
            else:
                if word2 in second_word.keys():
                    word3 = sample_word(second_word[word2])
                else:
                    word3_keys = np.random.choice(list(transitions.values()), 1)
                    word3 = sample_word(word3_keys)

        sentence.append(word3)
        word0 = word1
        word1 = word2
        word2 = word3
        no_of_words = no_of_words - 1
    print(' '.join(sentence))

def train_markov_model(data):
    for line in data:
        line_length = len(line)
        for i in range(line_length):
            token = line[i]
            if i == 0:
                initial_word[token] = initial_word.get(token, 0) + 1
            else:
                prev_token = line[i-1]
                if i == 1:
                    add2dict(second_word, prev_token, token)
                elif i == 2:
                    second_token = line[i-2]
                    add2dict(transitions, (second_token, prev_token), token)
                    add2dict(second_word, prev_token, token)
                    add2dict(second_word, second_token, prev_token)
                else:
                    third_token = line[i-3]
                    second_token = line[i-2]
                    add2dict(transitions, (second_token, prev_token), token)
                    add2dict(transitions, (third_token, second_token, prev_token), token)
                    add2dict(transitions, (third_token, prev_token), token)
                    add2dict(transitions, (third_token, second_token), prev_token)
                    add2dict(second_word, prev_token, token)
                    add2dict(second_word, second_token, prev_token)
                    add2dict(second_word, third_token, second_token)
    
    # Normalize the distributions
    initial_word_total = sum(initial_word.values())
    for key, value in initial_word.items():
        initial_word[key] = value / initial_word_total
        
    for prev_word, next_word_list in second_word.items():
        second_word[prev_word] = list2probabilitydict(next_word_list)
        
    for word_pair, next_word_list in transitions.items():
        transitions[word_pair] = list2probabilitydict(next_word_list)
    


data = pd.read_csv('text_matrix.csv', dtype=type('string') ,header=None).values
data = np.array([row[~pd.isnull(row)] for row in data])
initial_word = {}
second_word = {}
transitions = {}
train_markov_model(data)
sentence = 'cocoa bahia'
generate(sentence,2)
