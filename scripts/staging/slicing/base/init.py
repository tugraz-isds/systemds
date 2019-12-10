import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder


def init():
    file_name = 'salary.csv'
    # hot encoded dataset and getting target Y array
    dataset = pd.read_csv(file_name)
    attributes_amount = len(dataset.values[0])
    # for now working with regression datasets, assuming that target attribute is the last one
    # currently non-categorical features are not supported and should be binned
    y = dataset.iloc[:, attributes_amount - 1:attributes_amount].values
    # starting with one not including id field
    x = dataset.iloc[:, 1:attributes_amount - 1].values
    # list of numerical columns
    non_categorical = [4, 5]
    for row in x:
        for attribute in non_categorical:
            # <attribute - 2> as we already excluded from x id column
            row[attribute - 2] = int(row[attribute - 2] / 5)
    # hot encoding of categorical features
    enc = OneHotEncoder(handle_unknown='ignore')
    x = enc.fit_transform(x).toarray()
    complete_x = []
    complete_y = []
    counter = 0
    for item in x:
        complete_row = (counter, item)
        complete_x.append(complete_row)
        complete_y.append((counter, y[counter]))
        counter = counter + 1
    x_size = counter
    # train model on a whole dataset
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3, random_state=0)
    model = LinearRegression()
    model.fit(x_train, y_train)
    f_l2 = 0
    for i in range(0, len(y_test)):
        prediction = model.predict(x_test[i].reshape(1, -1))
        f_l2_sample = (prediction - y_test[i]) ** 2
        f_l2 = f_l2 + float(f_l2_sample)
    f_l2 = f_l2 / x_size
    return enc, model, complete_x, complete_y, f_l2, x_size, x_test, y_test
