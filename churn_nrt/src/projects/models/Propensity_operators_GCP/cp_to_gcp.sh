#!/usr/bin/env bash

BUCKET=vf-es-ca-nonlive-dev

cd /Users/gutierrezm/Projects/CBU/models
zip -r my_propensity_operators.zip my_propensity_operators

gsutil cp my_propensity_operators.zip gs://${BUCKET}/models/propensity_operators/my_propensity_operators.zip
gsutil cp my_propensity_operators/model/model.py gs://${BUCKET}/models/propensity_operators/model.py
gsutil cp my_propensity_operators/model/predictions.py gs://${BUCKET}/models/propensity_operators/predictions.py
gsutil cp my_propensity_operators/resources/configs.yml gs://${BUCKET}/models/propensity_operators/configs.yml
