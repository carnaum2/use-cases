Propensity operators model
============================

> This model tries to predict the propensity of our customers to the following groups of competitors: MásMóvil, Movistar, Orange and Other operators. 
> The model is trained with customers who have already churned to one of those operators.
> The labelling is based on the portouts within 30 days since the date chosen for training the model
> There is one binary model per competitor. The operator for which the customer has a higher score will be the final prediction

### Structure of the project

 
    ├── data
    │   └── get_dataset.py        # Function which returns the ids labelled for each operator
    ├── model                    
    │   ├── __init.py__         
    │   ├── model.py              # Build a binary model for each operator and save predictions
    │   └── predictions.py        # Join each model's predictions in order to get final predictions. Get multiclass metrics and save the final predictions dataframe
    └── resources
        ├── configs.yml           # Input and output paths (for GCP and BDP)
        └── input_vars.py         # List of input variables for each operator
