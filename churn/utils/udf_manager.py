





class Funct_to_UDF:

    @staticmethod
    def convert_to_date(dd_str):
        import datetime as dt
        if dd_str in [None, ""] or dd_str!=dd_str: return None
        dd_obj = dt.datetime.strptime(dd_str.replace("-", "").replace("/", ""), "%Y%m%d")
        if dd_obj < dt.datetime.strptime("19000101", "%Y%m%d"):
            return None
        return dd_obj.strftime("%Y-%m-%d %H:%M:%S") if dd_str and dd_str == dd_str else dd_str