## ThesisFPiet
Repository including queries and code

# Data aggregation
The set which is put into the model is the set for which one price elasticity coefficient is estimated. Hence, if product-specific, time-specific, category-specific, or any other specific elasticity is preferred, this should already be filtered in the data.
The aggregation of the data is that every observation represents the data of one week regarding a product within a price line.

The model has a partially linear form, in which the dependent variable (log(demand)) is explained by the treatment variable (log(selling price)). In the model other (log-transformed) covariates are included in a possibly non-parametric function. As endogeneity might be present due to confounding variables within the covariates explainign both price and demand, also an instrumental variable is included (log(purchase price)). As the covariates might enter the demand equation in a non-linear fashion, the DML method is used.

# Method (in short)
In the DML method, 2 stages are executed. In the first stage, using a sample-splitting method, the three main variables (demand, sell price and purcahse price) are estimated by the other covariates present in the model. These estimations are then used to residualize the variables by subtracting the predictions from the observed values. In the second stage, these adjusted variables are then assumed to be orthogonal to the covariates, and subsequently, the effect of the adjusted selling price on adjusted demand (a.k.a. the price elasticity) is estimated using the adjusted instrumental variable (IV regression).


# Data Prep
Before the data can be put into a model, I used a script to transform the data for the data to be further prepped. Hereby a "checklist" for what needs to be done before it can be put into a model:  
- remove outliers (as you probably already did)  
- create lags of relevant fields (product-specific demand is specifically interesting)  
- make a variable that counts the number of weeks from the start of the set (as a numerical inflation variable)  
- include squared values of variables (f.e. the aforementioned inflation variable, unavailability percentage, anything that makes sense in your opinion)  
- remove zero demand observations (after creating lags -> zero demand in lag will be handled differently *)  
  - the zero demand observations most probably refer to a week of promotions, in which regular demand is therefore zero, however the product is probably sold (against a discounted value)
- log-transform all the numerical variables (after removing zero demand observations --> otherwise they become `-infinity`)  
- replace the lagged variables that have value `-infinity` (*) with a mean value of demand  
- create dummy (0/1) variables out of the non-numerical features (and leave 1 default dummy per categorical variable out: if you include all dummies you cannot work with it)  
- include a constant  
    
  So then all the relevant variables are ready and transformed; and it's important that all input variables are numerical (either a log-transformed value or a dummy variable) and there are no `-infinity` values left. `NaN`  should be okay when the XGBRegressor is used in the first stage.
