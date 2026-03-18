import pandas as pd
import os

# File paths
DATA_DIR = '../data'
EVENTS_FILE = os.path.join(DATA_DIR, 'events.csv')
CAMPAIGNS_FILE = os.path.join(DATA_DIR, 'campaigns.csv')
MESSAGES_FILE = os.path.join(DATA_DIR, 'messages.csv')
PURCHASE_FILE = os.path.join(DATA_DIR, 'client_first_purchase_date.csv')
FRIENDS_FILE = os.path.join(DATA_DIR, 'friends.csv')

def clean_events():
    print("Cleaning events.csv...")
    try:
        # Read data. low_memory=False prevents type errors in mixed columns
        df = pd.read_csv(EVENTS_FILE, low_memory=False)
        
        # Convert time to datetime format
        df['event_time'] = pd.to_datetime(df['event_time'], errors='coerce')
        
        # Fill empty categories and brands with 'unknown'
        df['category_code'] = df['category_code'].fillna('unknown')
        df['brand'] = df['brand'].fillna('unknown')
        
        # Drop rows where user_id or product_id is missing (garbage data)
        df = df.dropna(subset=['user_id', 'product_id'])
        
        # Save the cleaned file
        df.to_csv(os.path.join(DATA_DIR, 'events_cleaned.csv'), index=False)
        print(f"Done! Rows: {len(df)}")
    except FileNotFoundError:
        print(f"File {EVENTS_FILE} not found.")

def clean_campaigns():
    print("Cleaning campaigns.csv...")
    try:
        df = pd.read_csv(CAMPAIGNS_FILE)
        
        # Convert dates
        df['started_at'] = pd.to_datetime(df['started_at'], errors='coerce')
        df['finished_at'] = pd.to_datetime(df['finished_at'], errors='coerce')
        
        # Fill empty boolean values with False (prevents NULL errors in DB)
        bool_cols =['ab_test', 'warmup_mode', 'subject_with_personalization', 
                     'subject_with_deadline', 'subject_with_emoji', 'subject_with_bonuses', 
                     'subject_with_discount', 'subject_with_saleout', 'is_test']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].fillna(False)
                
        # Fill empty hour limits and positions with 0
        df['hour_limit'] = df['hour_limit'].fillna(0)
        df['position'] = df['position'].fillna(0)
        
        df.to_csv(os.path.join(DATA_DIR, 'campaigns_cleaned.csv'), index=False)
        print(f"Done! Rows: {len(df)}")
    except FileNotFoundError:
        print("File campaigns.csv not found.")

def clean_messages():
    print("Cleaning messages.csv...")
    try:
        df = pd.read_csv(MESSAGES_FILE, low_memory=False)
        
        # List of date columns (from assignment description)
        date_cols =['date', 'sent_at', 'opened_first_time_at', 'opened_last_time_at', 
                     'clicked_first_time_at', 'clicked_last_time_at', 'unsubscribed_at', 
                     'hard_bounced_at', 'soft_bounced_at', 'complained_at', 
                     'blocked_at', 'purchased_at']
        
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        # Convert empty flags to False
        bool_cols =['is_opened', 'is_clicked', 'is_unsubscribed', 'is_hard_bounced', 
                     'is_soft_bounced', 'is_complained', 'is_blocked', 'is_purchased']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].fillna(False).astype(bool)
                
        # Fill empty email providers, platforms, and streams with 'unknown'
        df['email_provider'] = df['email_provider'].fillna('unknown')
        df['platform'] = df['platform'].fillna('unknown')
        df['stream'] = df['stream'].fillna('unknown')
        
        df.to_csv(os.path.join(DATA_DIR, 'messages_cleaned.csv'), index=False)
        print(f"Done! Rows: {len(df)}")
    except FileNotFoundError:
        print("File messages.csv not found.")

def clean_others():
    print("Cleaning friends.csv and client_first_purchase_date.csv...")
    try:
        # Friends
        df_f = pd.read_csv(FRIENDS_FILE)
        df_f = df_f.dropna() # Remove rows with missing relations
        df_f.to_csv(os.path.join(DATA_DIR, 'friends_cleaned.csv'), index=False)
        
        # Purchases
        df_p = pd.read_csv(PURCHASE_FILE)
        # Find the date column dynamically if it's named 'date' or 'first_purchase_date'
        date_col = [col for col in df_p.columns if 'date' in col.lower()]
        if date_col:
            df_p[date_col[0]] = pd.to_datetime(df_p[date_col[0]], errors='coerce')
        df_p = df_p.dropna()
        df_p.to_csv(os.path.join(DATA_DIR, 'client_first_purchase_date_cleaned.csv'), index=False)
        print("Done!")
    except FileNotFoundError as e:
        print(f"File not found: {e}")

if __name__ == "__main__":
    #print("Starting data cleaning.")
    clean_events()
    clean_campaigns()
    clean_messages()
    clean_others()
    print("All data successfully cleaned and saved with '_cleaned.csv' suffix!")