args<-commandArgs(TRUE)
options(digits=22)
library("Matrix")
library("nnet")

X = as.matrix(readMM(paste(args[1], "A.mtx", sep="")))
Y = as.matrix(readMM(paste(args[1], "B.mtx", sep="")))
X_test = as.matrix(readMM(paste(args[1], "C.mtx", sep="")))
Y_test = as.matrix(readMM(paste(args[1], "D.mtx", sep="")))

X = cbind(Y, X)
X_test = cbind(Y_test, X_test)
X = as.data.frame(X)
# set a baseline variable
X$V1 <- relevel(as.factor(X$V1), ref = "3")
X_test = as.data.frame(X_test)
model = multinom(V1~., data = X) # train model
pred <- predict(model, newdata = X_test, "class") # predict unknown data
acc = (sum(pred == Y_test)/nrow(Y_test))*100

writeMM(as(as.matrix(acc), "CsparseMatrix"), paste(args[2], "O", sep=""))