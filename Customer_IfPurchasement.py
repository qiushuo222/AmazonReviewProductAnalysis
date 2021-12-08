import pandas as pd
from sklearn import svm
import joblib
from sklearn import metrics,model_selection,preprocessing
import os
import sys
import joblib
'''
Input ==> Json file name got from Customer_IfPurchasement_Data_Preparation.py
    +---------------------+-----------+--------------------+-------------+------------+-------------+-----------+----+-----------------+
    |Product_Main_Category|Cat_Encoder|       Product_Brand|Brand_Encoder|Product_Rank|Product_Price|Review_Vote|Rate|Product_Purchased|
    +---------------------+-----------+--------------------+-------------+------------+-------------+-----------+----+-----------------+
    |            Computers|       13.0|                Dell|       3961.0|     14269.0|         76.4|        0.0| 3.0|                1|
    |            Computers|       13.0|          Easy Style|       4606.0|     21056.0|        24.47|        0.0| 5.0|                1|
    |            Computers|       13.0|             Samsung|      13084.0|     10109.0|       399.95|        7.0| 1.0|               -1|
    |            Computers|       13.0|                 ZTC|      16827.0|       290.0|         0.37|        0.0| 4.0|                1|
    |            Computers|       13.0|             ProCase|      11853.0|        95.0|        15.99|        0.0| 5.0|                1|
    |            Computers|       13.0|SilverStone Techn...|      13485.0|      3854.0|         2.54|        0.0| 5.0|                1|
    |            Computers|       13.0|                ZAGG|      16792.0|       188.0|        54.99|        0.0| 3.0|                1|
    |            Computers|       13.0|           Transcend|      15198.0|       656.0|        13.99|        0.0| 5.0|                1|
    |            Computers|       13.0|           Medialink|       9657.0|      8466.0|         6.31|        3.0| 1.0|                1|
    |            Computers|       13.0|       Cooler Master|       3324.0|       383.0|        15.92|        0.0| 1.0|                1|
    |            Computers|       13.0|             Samsung|      13084.0|     10109.0|       399.95|        0.0| 5.0|                1|
    |            Computers|       13.0|      Bargains Depot|       1618.0|      2176.0|         6.99|        0.0| 5.0|               -1|
    |            Computers|       13.0|               Wacom|      16116.0|      6264.0|         4.07|        0.0| 3.0|                1|
    +---------------------+-----------+--------------------+-------------+------------+-------------+-----------+----+-----------------+

Output ==> SVM Model for prediction

Make Prediction require read two encoders json to build up data
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
    x_train, x_test, y_train, y_test = model_selection.train_test_split(X,Y, train_size=0.95, test_size=0.05)
    min_max_scaler = preprocessing.MinMaxScaler()
    
    x_train = min_max_scaler.fit_transform(x_train)
    x_test = min_max_scaler.fit_transform(x_test)

    return x_train, y_train, x_test, y_test


def main(Path, Output_Path):

    x_train, y_train, x_test, y_test = read_data(Path)
    model = svm.SVC(C = 1.0, kernel='rbf', gamma='auto', decision_function_shape='ovr', cache_size=1000, random_state = 1000, max_iter=1000)
    model.fit(x_train, y_train)
    print("Train set accuracy is " + str(model.score(x_train, y_train)))
    print("Test set accuracy is " + str(model.score(x_test, y_test)))

    tn_train, fp_train, fn_train, tp_train = metrics.confusion_matrix(y_train, model.predict(x_train)).ravel()
    tn_val, fp_val, fn_val, tp_val = metrics.confusion_matrix(y_test, model.predict(x_test)).ravel()

    print("Train positive correct number is {}, train positive wrong number is {}.\n Train negative correct number is {}, train negative wrong number is {}".format(tp_train, fp_train, tn_train, fn_train))
    print("Test positive correct number is {}, test positive wrong number is {}.\n Test negative correct number is {}, test negative wrong number is {}".format(tp_val, fp_val, tn_val, fn_val))

    _ = joblib.dump(model, Output_Path)

if __name__ == '__main__':

    Customer_IfPurchase_Dataset = sys.argv[1]
    Output_Path = sys.argv[2]
    Path =  Customer_IfPurchase_Dataset

    main(Path, Output_Path)