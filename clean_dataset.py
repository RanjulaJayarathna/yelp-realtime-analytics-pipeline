import json
import csv
import time

# CONFIGURATION 
BUSINESS_FILE = 'yelp_academic_dataset_business.json'
REVIEW_FILE = 'yelp_academic_dataset_review.json'

OUTPUT_BUSINESS_CSV = 'cleaned_yelp_restaurants.csv'
OUTPUT_REVIEW_CSV = 'sampled_yelp_reviews.csv'

REVIEW_LIMIT = 10000  # Stop after 10,000 reviews 

def process_business_data():
    print(f"--- PART 1: Processing {BUSINESS_FILE} ---")
    start_time = time.time()
    
    with open(OUTPUT_BUSINESS_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['business_id', 'name', 'city', 'state', 'stars', 'review_count', 'categories', 'WiFi_Status']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        count = 0
        processed = 0
        
        with open(BUSINESS_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    
                    # CLEANING: Skip if categories is missing
                    if data['categories'] is None:
                        continue
                    
                    # FILTER: Keep only Open Restaurants
                    if 'Restaurants' in data['categories'] and data['is_open'] == 1:
                        
                        # TRANSFORMATION: Extract nested WiFi attribute
                        wifi = 'No Info'
                        if data.get('attributes') and 'WiFi' in data['attributes']:
                            wifi = data['attributes']['WiFi']
                        
                        writer.writerow({
                            'business_id': data['business_id'],
                            'name': data['name'],
                            'city': data['city'],
                            'state': data['state'],
                            'stars': data['stars'],
                            'review_count': data['review_count'],
                            'categories': data['categories'],
                            'WiFi_Status': wifi
                        })
                        processed += 1
                except Exception:
                    continue
                
                count += 1
                if count % 50000 == 0:
                    print(f"Scanned {count} businesses...")

    print(f"DONE. Extracted {processed} active restaurants to '{OUTPUT_BUSINESS_CSV}'")
    print(f"Time taken: {round(time.time() - start_time, 2)} seconds.\n")


def process_review_data():
    print(f"--- PART 2: Processing {REVIEW_FILE} ---")
    print(f"Sampling first {REVIEW_LIMIT} rows only...")
    start_time = time.time()
    
    with open(OUTPUT_REVIEW_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['review_id', 'business_id', 'stars', 'date', 'text_clean']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        count = 0
        
        with open(REVIEW_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    
                    # CLEANING: Remove newlines from text to prevent CSV corruption
                    clean_text = data['text'].replace('\n', ' ').replace('\r', ' ')
                    
                    # TRANSFORMATION
                    clean_text = clean_text[:300]
                    
                    writer.writerow({
                        'review_id': data['review_id'],
                        'business_id': data['business_id'],
                        'stars': data['stars'],
                        'date': data['date'],
                        'text_clean': clean_text
                    })
                    
                    count += 1
                    if count >= REVIEW_LIMIT:
                        break
                        
                except Exception:
                    continue

    print(f"DONE. Saved {count} cleaned reviews to '{OUTPUT_REVIEW_CSV}'")
    print(f"Time taken: {round(time.time() - start_time, 2)} seconds.\n")

# MAIN  
if __name__ == "__main__":
    print("STARTING DATA PIPELINE...\n")
    
    # Run Part 1
    process_business_data()
    
    # Run Part 2
    process_review_data()
    
