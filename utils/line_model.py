from sklearn import datasets, linear_model

def get_linear_model(X_parameters, Y_parameters, predict_value):
    # Create linear regression object
    regr = linear_model.LinearRegression()
    regr.fit(X_parameters, Y_parameters)
    predict_outcome = regr.predict(predict_value)
    predictions = {}
    predictions['intercept'] = regr.intercept_
    predictions['coefficient'] = regr.coef_
    predictions['predicted_value'] = predict_outcome
    return predict_outcome

if __name__ == '__main__':
    X_parameters = [[425.0], [432], [437.0], [452.0]]
    Y_parameters = [1, 2, 3, 4]
    get_linear_model(X_parameters,Y_parameters,900)
