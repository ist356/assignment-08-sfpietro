import pandas as pd
import streamlit as st 


def top_locations(violations_df : pd.DataFrame, threshold=1000) -> pd.DataFrame:
    pivot_df = violations_df.pivot_table(index='location', values='amount', aggfunc='sum')
    top_df = pivot_df[pivot_df['amount'] >= threshold]
    top_df = top_df.sort_values(by='amount', ascending=False)
    top_df['location'] = top_df.index
    top_df = top_df.reset_index(drop=True)
    return top_df


def top_locations_mappable(violations_df : pd.DataFrame, threshold=1000) -> pd.DataFrame:
    top_df = top_locations(violations_df, threshold)
    combined = pd.merge(top_df, violations_df, left_on='location', 
                        right_on='location')
    top_loc_df = combined[['location', 'amount_x', 'lat', 'lon']]
    top_loc_dedupe_df = top_loc_df.drop_duplicates(subset='location')
    top_loc_dedupe_df = top_loc_dedupe_df.rename(columns={'amount_x': 'amount'})
    return top_loc_dedupe_df

def tickets_in_top_locations(violations_df : pd.DataFrame, threshold=1000) -> pd.DataFrame:
    top = top_locations(violations_df, threshold)
    del top['amount']
    combined = pd.merge(top, violations_df, left_on='location', right_on='location')
    return combined

if __name__ == '__main__':
    '''
    Main ETL job. 
    '''
    import streamlit as st
    import pandas as pd
    df = pd.read_csv('./cache/final_cuse_parking_violations.csv')
    top = top_locations(df)
    top.to_csv('./cache/top_locations.csv', index=False)
    top_map = top_locations_mappable(df)
    top_map.to_csv('./cache/top_locations_mappable.csv', index=False)
    top_locations = tickets_in_top_locations(df)
    top_locations.to_csv('./cache/tickets_in_top_locations.csv', index=False)
    st.write(top_map)