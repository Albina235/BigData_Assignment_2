-- Drop existing tables
DROP TABLE IF EXISTS messages;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS campaigns;
DROP TABLE IF EXISTS friends;
DROP TABLE IF EXISTS client_first_purchase;

-- 1. Create Campaigns table
CREATE TABLE campaigns (
    id VARCHAR(255),
    campaign_type VARCHAR(50),
    channel VARCHAR(50),
    topic VARCHAR(255),
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    total_count FLOAT, -- Changed to FLOAT to handle '48211.0' from Pandas
    ab_test BOOLEAN,
    warmup_mode BOOLEAN,
    hour_limit FLOAT,
    subject_length FLOAT,
    subject_with_personalization BOOLEAN,
    subject_with_deadline BOOLEAN,
    subject_with_emoji BOOLEAN,
    subject_with_bonuses BOOLEAN,
    subject_with_discount BOOLEAN,
    subject_with_saleout BOOLEAN,
    is_test BOOLEAN,
    position FLOAT -- Changed to FLOAT just in case Pandas added .0 to empty positions
);

-- 2. Create Messages table (Updated with exact columns from the real dataset)
CREATE TABLE messages (
    id VARCHAR(255),
    message_id VARCHAR(255),
    campaign_id VARCHAR(255),
    message_type VARCHAR(50),
    client_id VARCHAR(255),
    channel VARCHAR(50),
    category VARCHAR(255),
    platform VARCHAR(100),
    email_provider VARCHAR(255),
    stream VARCHAR(100),
    date TIMESTAMP, -- TIMESTAMP is safer for Pandas date formats
    sent_at TIMESTAMP,
    is_opened BOOLEAN,
    opened_first_time_at TIMESTAMP,
    opened_last_time_at TIMESTAMP,
    is_clicked BOOLEAN,
    clicked_first_time_at TIMESTAMP,
    clicked_last_time_at TIMESTAMP,
    is_unsubscribed BOOLEAN,
    unsubscribed_at TIMESTAMP,
    is_hard_bounced BOOLEAN,
    hard_bounced_at TIMESTAMP,
    is_soft_bounced BOOLEAN,
    soft_bounced_at TIMESTAMP,
    is_complained BOOLEAN,
    complained_at TIMESTAMP,
    is_blocked BOOLEAN,
    blocked_at TIMESTAMP,
    is_purchased BOOLEAN,
    purchased_at TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    user_device_id VARCHAR(255),
    user_id VARCHAR(255)
);

-- 3. Create Events table
CREATE TABLE events (
    event_time TIMESTAMP,
    event_type VARCHAR(50),
    product_id VARCHAR(255), -- Using VARCHAR to avoid .0 parsing errors
    category_id VARCHAR(255),
    category_code VARCHAR(255),
    brand VARCHAR(255),
    price FLOAT,
    user_id VARCHAR(255),
    user_session VARCHAR(255)
);

-- 4. Create Friends table
CREATE TABLE friends (
    user_id_1 VARCHAR(255),
    user_id_2 VARCHAR(255)
);

-- 5. Create Client First Purchase table (Updated with missing columns)
CREATE TABLE client_first_purchase (
    client_id VARCHAR(255),
    first_purchase_date TIMESTAMP,
    user_id VARCHAR(255),
    user_device_id VARCHAR(255)
);

-- DATA LOADING BLOCK 
COPY campaigns FROM '/data/campaigns_cleaned.csv' DELIMITER ',' CSV HEADER;
COPY messages FROM '/data/messages_cleaned.csv' DELIMITER ',' CSV HEADER;
COPY events FROM '/data/events_cleaned.csv' DELIMITER ',' CSV HEADER;
COPY friends FROM '/data/friends_cleaned.csv' DELIMITER ',' CSV HEADER;
COPY client_first_purchase FROM '/data/client_first_purchase_date_cleaned.csv' DELIMITER ',' CSV HEADER;