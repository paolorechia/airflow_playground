import pandas as pd

def remotive_by_location(df: pd.DataFrame):
    return df[
        (df["location"].str.contains("europe"))
        | (df["location"].str.contains("germany"))
        | (df["location"].str.contains("worldwide"))
    ]
