import pandas as pd
import sys
import os

def load_data(tags_file="tags.parquet", alias_file="tag_alias.csv"):
    """Load tags and alias data."""
    print("Loading data...")
    try:
        df_tags = pd.read_parquet(tags_file)
        df_alias = pd.read_csv(alias_file)
        return df_tags, df_alias
    except Exception as e:
        print(f"Error loading files: {e}")
        sys.exit(1)

def filter_tags(df_tags):
    """Filter tags by post_count and drop unnecessary columns."""
    print(f"Original tags shape: {df_tags.shape}")
    
    # Drop columns
    cols_to_drop = ['created_at', 'updated_at', 'index', 'words']
    current_drop = [c for c in cols_to_drop if c in df_tags.columns]
    df_tags = df_tags.drop(columns=current_drop)
    print(f"Dropped columns: {current_drop}")
    
    # Filter by post_count >= 20
    initial_count = len(df_tags)
    df_tags = df_tags[df_tags['post_count'] >= 20].copy()
    print(f"Filtered rows: {initial_count} -> {len(df_tags)} (removed {initial_count - len(df_tags)})")
    
    # Sort by post_count descending
    df_tags = df_tags.sort_values(by='post_count', ascending=False)
    return df_tags

def process_aliases(df_alias):
    """Process alias data into a mapping."""
    print("Processing aliases...")
    df_alias['alias'] = df_alias['alias'].fillna('')
    df_alias['alias_list'] = df_alias['alias'].astype(str).str.split(',')
    
    def clean_alias_list(lst):
        if not isinstance(lst, list): return []
        return [x.strip() for x in lst if x and x.strip()]

    df_alias['alias_list'] = df_alias['alias_list'].apply(clean_alias_list)
    
    alias_map = df_alias.groupby('tag')['alias_list'].sum().reset_index()
    return alias_map

def merge_and_finalize(df_tags, alias_map):
    """Merge tags with aliases and finalize the dataframe."""
    print("Merging aliases...")
    merged_df = df_tags.merge(alias_map, left_on='name', right_on='tag', how='left')
    
    merged_df['alias'] = merged_df['alias_list'].apply(lambda x: x if isinstance(x, list) else [])
    
    merged_df = merged_df.drop(columns=['tag', 'alias_list'])
    
    # Reorder columns
    cols = merged_df.columns.tolist()
    desired_order = ['id', 'name', 'alias', 'post_count', 'category']
    remaining = [c for c in cols if c not in desired_order]
    final_cols = desired_order + remaining
    final_cols = [c for c in final_cols if c in merged_df.columns]
    
    return merged_df[final_cols]

def save_data(df, output_file="tags_processed.parquet"):
    """Save the processed dataframe."""
    df.to_parquet(output_file, index=False)
    print(f"Saved processed data to {output_file}. Rows: {len(df)}")

def main():
    df_tags, df_alias = load_data()
    df_tags = filter_tags(df_tags)
    alias_map = process_aliases(df_alias)
    final_df = merge_and_finalize(df_tags, alias_map)
    save_data(final_df)

if __name__ == "__main__":
    main()
