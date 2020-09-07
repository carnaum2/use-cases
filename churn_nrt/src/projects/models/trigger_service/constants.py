#!/usr/bin/env python
# -*- coding: utf-8 -*-

INSERT_TOP_K = 50000
EXTRA_INFO_COLS = ['flag_1w', 'flag_1w_tickets', 'flag_1w_orders','flag_1w_billing', 'flag_1w_reimbursements', 'flag_2w', 'flag_2w_tickets', 'flag_2w_orders','flag_2w_billing', 'flag_2w_reimbursements']

OWNER_LOGIN = "asaezco"

MODEL_OUTPUT_NAME = "triggers_ml"

CAR_PATH = "/data/udf/vf_es/churn/triggers/car_exploration_model1_segment_tickets_evol/"
CAR_PATH_UNLABELED = "/data/udf/vf_es/churn/triggers/car_exploration_unlabeled_model1_segment_tickets_evol/"