#!/usr/bin/env python
# -*- coding: utf-8 -*-

CORRELATED_FEATS = []


TARIFF_INC_DICT = {"TCP8M":[3.0,17.64],\
                  "TMIN0":[2.0,16.66],\
                  "T2C1G":[4.0,13.79],\
                  "TIL2G":[5.0,12.82],\
                  "TIL4G":[5.0,10.20],\
                  "TI10G":[5.0,7.24],\
                  "B4TAR":[5.0,14,70],\
                  "TARXX":[5.0,5.50],\
                  "TSP5M":[0.0,0.0],\
                  "TSP1G":[3.0,20.0],\
                  "TS215":[4.0,20.0],\
                  "TIS3G":[5.0,15.65],\
                  "TIS6G":[5.0,11.90],\
                  "TISET":[0.0, 0.0],\
                  "TARXS":[0.0, 0.0],\
                  "TRMPL":[5.0,11.36],\
                  "TRLPL":[5.0,8.47],\
                  "SYUTA":[3.0, 15.0],\
                  "TPYU0":[0.0, 0.0],\
                  "TYU15":[3.0,18.75],\
                  "T2C3G":[0.0,0.0],\
                  "TILOF":[0.0,0.0],\
                  "TISOF":[0.0,0.0],\
                  "TP2OF":[0.0,0.0],\
                  "TS2OF":[0.0,0.0],\
                  "TILRL":[0.0,0.0],\
                  "TPMNV":[0.0,0.0],\
                  "TARSS":[4.0,14.81],\
                  "TARRX":[5.0,7.04],\
                  "RXLTA":[5.0,8.77],\
                  "UNIXL":[5.0,15.62],\
                  "TSP8M":[3.0,20.0],\
                  "T2S1G":[4.0,20.0],\
                  "TIS2G":[5.0,16.66],\
                  "TIS4G":[5.0,11.90],\
                  "YUTAR":[3.0,9.67],\
                  "MEYTA":[3.0,12.0],\
                  "MTP1C":[0.0,0.0],\
                  "MTP1M":[0.0,0.0],\
                  "NTP1M":[4.0,23.55],\
                  "TN100":[4.0,23.39],\
                  "TN101":[4.0,23.39],\
                  "MTP2C":[0.0,0.0],\
                  "MTP2M":[5.0,18.46],\
                  "NTP2M":[4.0,13.75],\
                  "TN200":[4.0,31.10],\
                  "TN230":[4.0,28.42],\
                  "TN280":[4.0,22.59],\
                  "MTP3C":[0.0,0.0],\
                  "MTP3M":[0.0,0.0],\
                  "NTP3M":[5.0,12.14],\
                  "T300B":[5.0,47.89],\
                  "MTP4M":[0.0,0.0],\
                  "MTP5M":[5.0,9.38],\
                  "TP400":[5.0,29.23],\
                  "TN500":[5.0,29.23],\
                  "TA500":[5.0,28.24],\
                  "TP650":[0.0,0.0],\
                  "MRTPV":[0.0,0.0],\
                  "TILIM":[5.0,19.10],\
                  "TPCER":[0.0,0.0],\
                  "TPDVZ":[0.0,0.0],\
                  "TP630":[5.0,24.11],\
                  "T1000":[5.0,17.12],\
                  "T1020":[5.0,17.12],\
                  "T2LAR":[5.0,14.70],\
                  "T2LPL":[5.0,11.36],\
                  "T2LSS":[4.0,14.81],\
                  "T2LTA":[5.0,8.77],\
                  "T2L1G":[4.0,13.79],\
                  "T2L2G":[5.0,12.82],\
                  "T2L4G":[5.0,10.20]}

TARIFF_INC_FILT_DICT = {"TCP8M":[3.0,17.64],\
                  "TMIN0":[2.0,16.66],\
                  "T2C1G":[4.0,13.79],\
                  "TIL2G":[5.0,12.82],\
                  "TIL4G":[5.0,10.20],\
                  "TI10G":[5.0,7.24],\
                  "B4TAR":[5.0,14,70],\
                  "TARXX":[5.0,5.50],\
                  "TSP1G":[3.0,20.0],\
                  "TS215":[4.0,20.0],\
                  "TIS3G":[5.0,15.65],\
                  "TIS6G":[5.0,11.90],\
                  "TRMPL":[5.0,11.36],\
                  "TRLPL":[5.0,8.47],\
                  "SYUTA":[3.0, 15.0],\
                  "TYU15":[3.0,18.75],\
                  "TARSS":[4.0,14.81],\
                  "TARRX":[5.0,7.04],\
                  "RXLTA":[5.0,8.77],\
                  "UNIXL":[5.0,15.62],\
                  "TSP8M":[3.0,20.0],\
                  "T2S1G":[4.0,20.0],\
                  "TIS2G":[5.0,16.66],\
                  "TIS4G":[5.0,11.90],\
                  "YUTAR":[3.0,9.67],\
                  "MEYTA":[3.0,12.0],\
                  "NTP1M":[4.0,23.55],\
                  "TN100":[4.0,23.39],\
                  "TN101":[4.0,23.39],\
                  "MTP2M":[5.0,18.46],\
                  "NTP2M":[4.0,13.75],\
                  "TN200":[4.0,31.10],\
                  "TN230":[4.0,28.42],\
                  "TN280":[4.0,22.59],\
                  "NTP3M":[5.0,12.14],\
                  "T300B":[5.0,47.89],\
                  "MTP5M":[5.0,9.38],\
                  "TP400":[5.0,29.23],\
                  "TN500":[5.0,29.23],\
                  "TA500":[5.0,28.24],\
                  "TILIM":[5.0,19.10],\
                  "TP630":[5.0,24.11],\
                  "T1000":[5.0,17.12],\
                  "T1020":[5.0,17.12],\
                  "T2LAR":[5.0,14.70],\
                  "T2LPL":[5.0,11.36],\
                  "T2LSS":[4.0,14.81],\
                  "T2LTA":[5.0,8.77],\
                  "T2L1G":[4.0,13.79],\
                  "T2L2G":[5.0,12.82],\
                  "T2L4G":[5.0,10.20]}