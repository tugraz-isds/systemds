#load text matrix file
text_matrix <- read.csv(file="C:\\Users\\Afan\\Desktop\\DIA\\text_matrix.csv",header = FALSE, sep = ',')
text_matrix <- as.matrix(text_matrix)
print(text_matrix)