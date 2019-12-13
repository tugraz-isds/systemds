from bs4 import BeautifulSoup,SoupStrainer
import nltk
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import string
import numpy as np
import csv
#Change path to file
f = open('reut2-000.sgm', 'r')
data= f.read()
data = data[:1020]
lemmatizer = WordNetLemmatizer() 
soup = BeautifulSoup(data,'html.parser')
sentences = []
text_matrix = []
#Find the body of a file with sentences
for item in soup.findAll('body'):
    sentences = sent_tokenize(item.text)
    for sentence in sentences:
        #Remove stopwords and punctuation
        text_matrix.append([token for token in word_tokenize(sentence) if token.lower() not in stopwords.words('english') and token not in string.punctuation])
for i in range(len(text_matrix)):
    for j in range(len(text_matrix[i])):
        #Lemmatization
       text_matrix[i][j] = lemmatizer.lemmatize(text_matrix[i][j].lower(), pos='v')
length = max(map(len, text_matrix))
#Extend all rows to same length
text_matrix=np.array([row + [None] * (length - len(row)) for row in text_matrix])
#Write to csv file
for row in text_matrix:
    with open('C:\\Users\\Afan\\Desktop\\DIA\\text_matrix.csv', 'a', newline='') as csvFile:
        writer = csv.writer(csvFile)
        writer.writerow(row)
    csvFile.close()