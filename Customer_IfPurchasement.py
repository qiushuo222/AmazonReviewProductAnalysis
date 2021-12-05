import pandas as pd
from sklearn import svm
import joblib
from sklearn import metrics,model_selection
import os
import sys

'''
Input ==> Json file name got from Customer_IfPurchasement_Data_Preparation.py
Output ==> SVM Model for prediction
'''
def read_data(Path):
    data = pd.read_json(Path,lines=True)
    data["Product_Rank"] = data["Product_Rank"].astype("float64")
    data["Review_Vote"] = data["Review_Vote"].astype("float64")
    data["Brand_Encoder"] = data["Brand_Encoder"].astype("float64")
    data["Cat_Encoder"] = data["Cat_Encoder"].astype("float64")
    data["Rate"] = data["Rate"].astype("float64")
    X = data[["Cat_Encoder", "Brand_Encoder", "Product_Rank", "Product_Price", "Rate", "Review_Vote"]]
    Y = data["Product_Purchased"]
    X = X.to_numpy()
    Y = Y.to_numpy()
    x_train, x_test, y_train, y_test = model_selection.train_test_split(X,Y, train_size=0.98, test_size=0.02)
    x_train, x_val, y_train, y_val = model_selection.train_test_split(x_train, y_train, train_size=0.95, test_size=0.05)

    return x_train, y_train, x_val, y_val, x_test, y_test


def main(Path):

    x_train, y_train, x_val, y_val, x_test, y_test = read_data(Path)
    model = svm.SVC(C = 1.0, kernel='rbf', gamma='auto', decision_function_shape='ovr', cache_size=500, random_state = 1000)
    model.fit(x_train, y_train)
    print("Train set score" + str(model.score(x_train, y_train)))
    print("Validation set score" + str(model.score(x_val, y_val)))

    tn_train, fp_train, fn_train, tp_train = metrics.confusion_matrix(y_train, model.predict(x_train)).ravel()
    tn_val, fp_val, fn_val, tp_val = metrics.confusion_matrix(y_val, model.predict(x_val)).ravel()

    print("Train positive correct number is {}, train positive wrong number is {}.\n Train negative correct number is {}, train negative wrong number is {}".format(tp_train, fp_train, tn_train, fn_train))
    print("Validation positive correct number is {}, validation positive wrong number is {}.\n Validation negative correct number is {}, validation negative wrong number is {}".format(tp_val, fp_val, tn_val, fn_val))

if __name__ == '__main__':
    ffolder = os.path.abspath("")
    Customer_IfPurchase_Dataset = sys.argv[1]
    
    Path = os.path.join(ffolder, "CustomerIfPurchase_Dataset", Customer_IfPurchase_Dataset)

    main(Path)