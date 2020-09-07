


import pandas as pd
import os
from pyspark.sql.functions import collect_set, col, count as sql_count, lit, collect_list, desc, asc


DEFAULT_CELL_HEIGHT = 15 # pixels
DEFAULT_CELL_WIDTH = 20 # pixels

TITLE_FORMAT = {'bold': True, 'text_wrap': True, 'valign': 'top', 'fg_color': '#92CDDC', 'border': 2, 'font_size': 15}
SUBTITLE_FORMAT = {'bold': False, 'text_wrap': True, 'valign': 'top', 'fg_color': '#DAEEF3', 'font_size': 15}
TEXT_BOLD_FORMAT = {'bold': True, 'text_wrap': True, 'fg_color': '#DBDBDB', 'font_size': 10}

# Save the root path for savings
SAVING_PATH = os.path.join(os.environ.get('BDA_USER_HOME', ''), "data", "churn", "ccc_churn_analysis", "results")

FORMAT_DICT = {
    "title_format" : None,
    "subtitle_format" : None,
    "text_bold_format"  : None
}


def init_writer(final_filename):

    writer = pd.ExcelWriter(final_filename, engine='xlsxwriter')
    workbook = writer.book
    global FORMAT_DICT
    FORMAT_DICT["title_format"] = workbook.add_format(TITLE_FORMAT)
    FORMAT_DICT["subtitle_format"] = workbook.add_format(SUBTITLE_FORMAT)
    FORMAT_DICT["text_bold_format"] =  workbook.add_format(TEXT_BOLD_FORMAT)

    return writer


def compute_results(df_base_tgs_port_ccc):


    df_base_eop_ccc_churn_trigger_show = df_base_tgs_port_ccc.select("EOP", "CHURN", "TRIGGER").groupby("EOP", "CHURN", "TRIGGER").agg(sql_count("*").alias("count")).collect()

    base_eop_churn_trigger = [row_["count"] for row_ in df_base_eop_ccc_churn_trigger_show if row_["EOP"]==1 and row_["CHURN"]==1 and row_["TRIGGER"]==1]
    base_eop_churn_trigger = 0 if len(base_eop_churn_trigger)==0 else base_eop_churn_trigger[0]

    base_eop_churn_notrigger = [row_["count"] for row_ in df_base_eop_ccc_churn_trigger_show if row_["EOP"]==1 and row_["CHURN"]==1 and row_["TRIGGER"]==0]
    base_eop_churn_notrigger = 0 if len(base_eop_churn_notrigger)==0 else base_eop_churn_notrigger[0]

    base_eop_nochurn_trigger = [row_["count"] for row_ in df_base_eop_ccc_churn_trigger_show if row_["EOP"]==1 and row_["CHURN"]==0 and row_["TRIGGER"]==1]
    base_eop_nochurn_trigger = 0 if len(base_eop_nochurn_trigger)==0 else base_eop_nochurn_trigger[0]

    base_eop_nochurn_notrigger = [row_["count"] for row_ in df_base_eop_ccc_churn_trigger_show if row_["EOP"]==1 and row_["CHURN"]==0 and row_["TRIGGER"]==0]
    base_eop_nochurn_notrigger = 0 if len(base_eop_nochurn_notrigger)==0 else base_eop_nochurn_notrigger[0]

    base_noeop_churn_trigger = [row_["count"] for row_ in df_base_eop_ccc_churn_trigger_show if row_["EOP"]==0 and row_["CHURN"]==1 and row_["TRIGGER"]==1]
    base_noeop_churn_trigger = 0 if len(base_noeop_churn_trigger)==0 else base_noeop_churn_trigger[0]

    base_noeop_churn_notrigger = [row_["count"] for row_ in df_base_eop_ccc_churn_trigger_show if row_["EOP"]==0 and row_["CHURN"]==1 and row_["TRIGGER"]==0]
    base_noeop_churn_notrigger = 0 if len(base_noeop_churn_notrigger)==0 else base_noeop_churn_notrigger[0]

    base_noeop_nochurn_trigger = [row_["count"] for row_ in df_base_eop_ccc_churn_trigger_show if row_["EOP"]==0 and row_["CHURN"]==0 and row_["TRIGGER"]==1]
    base_noeop_nochurn_trigger = 0 if len(base_noeop_nochurn_trigger)==0 else base_noeop_nochurn_trigger[0]

    base_noeop_nochurn_notrigger = [row_["count"] for row_ in df_base_eop_ccc_churn_trigger_show if row_["EOP"]==0 and row_["CHURN"]==0 and row_["TRIGGER"]==0]
    base_noeop_nochurn_notrigger = 0 if len(base_noeop_nochurn_notrigger)==0 else base_noeop_nochurn_notrigger[0]

    base_eop_churn =  base_eop_churn_trigger +  base_eop_churn_notrigger
    base_eop_nochurn =  base_eop_nochurn_trigger + base_eop_nochurn_notrigger
    base_eop = base_eop_churn + base_eop_nochurn

    base_noeop_churn =  base_noeop_churn_trigger +  base_noeop_churn_notrigger
    base_noeop_nochurn =  base_noeop_nochurn_trigger + base_noeop_nochurn_notrigger
    base_noeop = base_noeop_churn + base_noeop_nochurn

    base_churn_trigger = base_noeop_churn_trigger + base_eop_churn_trigger
    base_nochurn_trigger = base_noeop_nochurn_trigger + base_eop_nochurn_trigger

    base_churn_notrigger = base_noeop_churn_notrigger + base_eop_churn_notrigger
    base_nochurn_notrigger = base_noeop_nochurn_notrigger + base_eop_nochurn_notrigger

    base_trigger = base_churn_trigger + base_nochurn_trigger
    base_notrigger = base_churn_notrigger + base_nochurn_notrigger


    base_population = base_eop + base_noeop

    base_churn = base_eop_churn + base_noeop_churn
    base_nochurn = base_eop_nochurn + base_noeop_nochurn

    base_eop_notrigger = base_eop_churn_notrigger + base_eop_nochurn_notrigger
    base_eop_trigger = base_eop_churn_trigger + base_eop_nochurn_trigger

    perc_churn_eop_notrigger = 1.0 * base_eop_churn_notrigger / base_eop_notrigger if base_eop_notrigger != 0 else 0
    perc_churn_eop_trigger = 1.0 * base_eop_churn_trigger / base_eop_trigger if base_eop_trigger != 0 else 0

    perc_churn_notrigger = 1.0 * base_churn_notrigger / base_notrigger if base_churn_notrigger != 0 else 0
    perc_churn_trigger = 1.0 * base_churn_trigger / base_trigger if base_trigger != 0 else 0

    from collections import OrderedDict

    content = OrderedDict([
        ("base_eop_churn_trigger",       base_eop_churn_trigger),
        ("base_eop_churn_trigger",       base_eop_churn_trigger),
        ("base_eop_churn_notrigger",     base_eop_churn_notrigger),
        ("base_eop_churn_notrigger",     base_eop_churn_notrigger),

        ("base_eop_nochurn_trigger",     base_eop_nochurn_trigger),
        ("base_eop_nochurn_trigger",     base_eop_nochurn_trigger),

        ("base_eop_nochurn_notrigger",   base_eop_nochurn_notrigger),
        ("base_eop_nochurn_notrigger",   base_eop_nochurn_notrigger),

        ("base_noeop_churn_trigger",     base_noeop_churn_trigger),
        ("base_noeop_churn_trigger",     base_noeop_churn_trigger),

        ("base_noeop_churn_notrigger",   base_noeop_churn_notrigger),
        ("base_noeop_churn_notrigger",   base_noeop_churn_notrigger),

        ("base_noeop_nochurn_trigger",   base_noeop_nochurn_trigger),
        ("base_noeop_nochurn_trigger",   base_noeop_nochurn_trigger),

        ("base_noeop_nochurn_notrigger", base_noeop_nochurn_notrigger),
        ("base_noeop_nochurn_notrigger", base_noeop_nochurn_notrigger),

        ("base_eop_churn",               base_eop_churn),
        ("base_eop_nochurn",             base_eop_nochurn),
        ("base_eop",                     base_eop),

        ("base_noeop_churn",             base_noeop_churn),
        ("base_noeop_nochurn",           base_noeop_nochurn),
        ("base_noeop",                   base_noeop),

        ("base_churn_trigger",           base_churn_trigger),
        ("base_nochurn_trigger",         base_nochurn_trigger),

        ("base_churn_notrigger",         base_churn_notrigger),
        ("base_nochurn_notrigger",       base_nochurn_notrigger),

        ("base_population",              base_population),

        ("base_churn",                   base_churn),
        ("base_nochurn",                 base_nochurn),

        ("base_eop_notrigger",           base_eop_notrigger),
        ("base_eop_trigger",             base_eop_trigger),

        ("base_trigger", base_trigger),
        ("base_notrigger", base_notrigger),

        ("perc_churn_eop_notrigger",     perc_churn_eop_notrigger),
        ("perc_churn_eop_trigger",       perc_churn_eop_trigger),
        ("perc_churn_notrigger", perc_churn_notrigger),
        ("perc_churn_trigger", perc_churn_trigger)

    ])

    df_churn_eop = pd.DataFrame({
                             "TRIGGER": perc_churn_eop_trigger,
                             "NO TRIGGER": perc_churn_eop_notrigger,
                             "LIFT" : perc_churn_eop_trigger/perc_churn_eop_notrigger}, index=[0])

    df_churn = pd.DataFrame({
        "TRIGGER": perc_churn_trigger,
        "NO TRIGGER": perc_churn_notrigger,
        "LIFT": perc_churn_trigger / perc_churn_notrigger}, index=[0])



    return content, df_churn, df_churn_eop



def print_sheet(sheet, content, writer):
    workbook = writer.book
    print("Writting sheet '{}'".format(sheet))
    worksheet = workbook.add_worksheet(sheet)
    writer.sheets[sheet] = worksheet
    worksheet.set_column("A:L", 25)

    row_ = 5
    row_margin = 2

    if isinstance(content, pd.DataFrame):
        content.to_excel(writer, sheet_name=sheet, startrow=row_, startcol=1, index=True, header=True)
        return True
    for key in content.keys():
        worksheet.write(row_, 0, key, FORMAT_DICT["text_bold_format"]) # key of the dict in a bold format
        if isinstance(content[key],dict):
            worksheet.write_row(row_, 1, ["{} : {}".format(k, v) for k, v in content[key].items()])
        else:
            worksheet.write(row_, 1, float(content[key]) if key.startswith("perc_") else int(content[key]))
        row_ += 1

    return True



