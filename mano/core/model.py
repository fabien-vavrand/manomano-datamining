import lightgbm as lgb


class LightGbmWrapper:
    """ LightGBM wrapper to fit a model after computing the best iteration using a cross validation """

    def __init__(self, params, categoricals='auto', cv=5, verbose=1):
        self.params = params
        self.categoricals = categoricals
        self.cv = cv
        self.verbose = verbose
        self.best_iteration = None
        self.score = None
        self.model = None

    def fit(self, X, y):
        dataset = lgb.Dataset(X, label=y, categorical_feature=self.categoricals, free_raw_data=False)
        cv = lgb.cv(self.params, dataset,
                    num_boost_round=5000,
                    early_stopping_rounds=50,
                    nfold=self.cv,
                    stratified=False, #required for regression
                    verbose_eval=self.verbose)
        self.best_iteration = len(cv['l2-mean'])
        self.score = cv['l2-mean'][-1]
        self.model = lgb.train(self.params, dataset, num_boost_round=self.best_iteration)
        return self

    def predict(self, X):
        if self.model is None:
            raise ValueError('Model is not fitted')

        return self.model.predict(X)