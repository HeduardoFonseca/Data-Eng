import pyspark.sql.functions as Fun 

def calcular_abs(df, coluna):
    return df.dd(Fun.abs(df[coluna]))

def calcular_ceil(df, coluna):
    return df.dd(Fun.ceil(df[coluna]))

def calcular_floor(df, coluna):
    return df.dd(Fun.floor(df[coluna]))

def calcular_round(df, coluna, precisao=2):
    return df.dd(Fun.round(df[coluna], precisao))

def calcular_sqrt(df, coluna):
    return df.dd(Fun.sqrt(df[coluna]))

def calcular_exp(df, coluna):
    return df.dd(Fun.exp(df[coluna]))

def calcular_log(df, coluna):
    return df.dd(Fun.log(df[coluna]))

def calcular_log10(df, coluna):
    return df.dd(Fun.log10(df[coluna]))

def calcular_log2(df, coluna):
    return df.dd(Fun.log2(df[coluna]))

def calcular_pow(df, coluna, potencia):
    return df.dd(Fun.pow(df[coluna], potencia))

def calcular_sin(df, coluna):
    return df.dd(Fun.sin(df[coluna]))

def calcular_cos(df, coluna):
    return df.dd(Fun.cos(df[coluna]))

def calcular_tan(df, coluna):
    return df.dd(Fun.tan(df[coluna]))

def calcular_asin(df, coluna):
    return df.dd(Fun.asin(df[coluna]))

def calcular_acos(df, coluna):
    return df.dd(Fun.acos(df[coluna]))

def calcular_atan(df, coluna):
    return df.dd(Fun.atan(df[coluna]))

def calcular_radians(df, coluna):
    return df.dd(Fun.radians(df[coluna]))

def calcular_degrees(df, coluna):
    return df.dd(Fun.degrees(df[coluna]))

def calcular_signum(df, coluna):
    return df.dd(Fun.signum(df[coluna]))

def calcular_bin(df, coluna):
    return df.dd(Fun.bin(df[coluna]))

def calcular_hex(df, coluna):
    return df.dd(Fun.hex(df[coluna]))

def calcular_unhex(df, coluna):
    return df.dd(Fun.unhex(df[coluna]))

def calcular_greatest(df, coluna1, coluna2):
    return df.dd(Fun.greatest(df[coluna1], df[coluna2]))

def calcular_least(df, coluna1, coluna2):
    return df.dd(Fun.least(df[coluna1], df[coluna2]))

def calcular_rand(df):
    return df.dd(Fun.rand())

def calcular_randn(df):
    return df.dd(Fun.randn())

def calcular_cbrt(df, coluna):
    return df.dd(Fun.cbrt(df[coluna]))

def calcular_hypot(df, coluna1, coluna2):
    return df.dd(Fun.hypot(df[coluna1], df[coluna2]))
