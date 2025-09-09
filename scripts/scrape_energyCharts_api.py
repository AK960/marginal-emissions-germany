import requests
import json
import pandas as pd

# Create parameters for the API request
start_date = "2025-01-01"
end_date = "2025-09-07"
country = "de"
type = "public_power"

# Make the API request
try:
    # Set URL dynamically
    url = f"https://api.energy-charts.info/{type}?country={country}&start={start_date}&end={end_date}"

    # Make the request
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()

        # Debug structure
        print(f"API Response Keys: {list(data.keys())}")

        try:
            # Create DataFrame
            df = pd.DataFrame({
                'timestamp': pd.to_datetime(data['unix_seconds'], unit='s', utc=True),
            })

            for production_type in data['production_types']:
                name = production_type['name']
                df[name] = production_type['data']

            # Füge deprecated-Flag hinzu falls vorhanden
            if 'deprecated' in data:
                df['deprecated'] = data['deprecated']

            # Safe to file
            df.to_csv(f"../data/ec_{type}_{start_date.replace("-", "")}_{end_date.replace("-", "")}_{country}.csv", index=False)

            print("Data saved successfully.")

        except Exception as e:
            print(f"Error: {e}")

except requests.exceptions.RequestException as e:
    print(f"Error: {e}")