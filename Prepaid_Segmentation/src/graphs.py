# -*- coding: utf-8 -*-
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns
import os
import src.pycharm_workspace.lib_csanc109.utils.custom_logger as clogger
logger = clogger.get_custom_logger()

class GraphGeneration():

    def __init__(self, save_path):
        self.save_path = save_path
        logger.info("Plots will be saved in '{}'".format(save_path))


    def plot_advanced_balance_evolution(self, df_graph, filename, start_date=None, end_date=None, saldo_column='bal_bod'):

        pp = PdfPages(os.path.join(self.save_path, filename))

        list_of_msisdn = df_graph["msisdn"].unique()

        for msisdn in list_of_msisdn:

            pdf_0 = df_graph[df_graph['msisdn'] == msisdn]

            plan = pdf_0["codigo_plan_precios"].iat[0]
            plan_desc = pdf_0["AddOn"].iat[0]
            cuota_plan = pdf_0["Cuota"].iat[0]


            # Set up plot.
            fig, ax = plt.subplots(figsize=(10, 6))
            ax.plot_date(x=pdf_0['fecha'], y=pdf_0[saldo_column], linestyle='-', marker='o', markersize=7)

            # Add vertical lines.
            tu_amount_field = "tu_amount" if "tu_amount" in pdf_0.keys() else "tu_sum_amounts"
            new_pdf = pdf_0[pdf_0[tu_amount_field] > 0]
            for _, row in new_pdf.iterrows():
                ax.vlines(x=row['fecha'],
                          ymin=row[saldo_column],
                          ymax=row[saldo_column] + row[tu_amount_field],
                          color='red')
                ax.plot(x=row['fecha'], ymin=row[saldo_column], marker="X", size=100, color='red')

            # Change graph y axis limit.

            ax.set_ylim(0, 1.05 * max([pdf_0[saldo_column].max(),
                                       pdf_0[tu_amount_field].max(),
                                       (pdf_0[saldo_column] + pdf_0[tu_amount_field]).max()]))

            # Add grid to plot.
            ax.grid(True, which='both', linestyle='--')

            # Set title.
            ax.set_title('Balance evolution for {msisdn} (plan={plan} - {plan_desc} - cuota={cuota})\n'.format(msisdn=msisdn, plan=plan,
                                                                                                               cuota=cuota_plan,
                                                                                               plan_desc=plan_desc), fontsize=18)

            # Set x and y axis lables.
            ax.set_ylabel('Balance', fontsize=16)
            ax.set_xlabel('Dates', fontsize=16)

            # Change x and y axis tick size, rotation and horizontal alignment.
            for tick in ax.yaxis.get_major_ticks():
                tick.label.set_fontsize(14)

            for tick in ax.xaxis.get_major_ticks():
                tick.label.set_fontsize(14)
                tick.label.set_rotation(45)
                tick.label.set_horizontalalignment('right')

            # Figure tight layout.
            fig.tight_layout()

            # Save plot.
            fig.savefig(pp, format='pdf')

        pp.close()

        logger.info("Generated file {}".format(os.path.join(self.save_path, filename)))