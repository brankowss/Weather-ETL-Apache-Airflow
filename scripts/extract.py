import os
import requests
import pandas as pd
import logging
from dotenv import load_dotenv
from time import sleep

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path)

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# API Config
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")
if not API_KEY:
    raise ValueError("API key not found in .env file")
else:
    logging.info("API key loaded successfully.")

# List of 100+ Chinese Cities
CITIES = [
    "Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu", "Chongqing", "Tianjin", "Wuhan", "Hangzhou", "Xi'an",
    "Nanjing", "Shenyang", "Harbin", "Suzhou", "Qingdao", "Dalian", "Zhengzhou", "Jinan", "Changsha", "Kunming",
    "Changchun", "Ürümqi", "Fuzhou", "Xiamen", "Hefei", "Taiyuan", "Shijiazhuang", "Nanchang", "Guiyang", "Nanning",
    "Hohhot", "Lanzhou", "Haikou", "Yinchuan", "Lhasa", "Macau", "Hong Kong", "Zhongshan", "Dongguan", "Wuxi",
    "Nantong", "Foshan", "Jinhua", "Zibo", "Luoyang", "Huaian", "Yantai", "Zhuhai", "Xuzhou", "Liuzhou", "Shaoxing",
    "Jiangmen", "Quanzhou", "Yangzhou", "Tangshan", "Weihai", "Baoding", "Changzhou", "Zhenjiang", "Huzhou", "Taizhou",
    "Linyi", "Handan", "Guilin", "Hengyang", "Zhuzhou", "Meizhou", "Maoming", "Jieyang", "Jingdezhen", "Jiujiang",
    "Leshan", "Neijiang", "Mianyang", "Zunyi", "Anshun", "Xingyi", "Yuxi", "Zhaotong", "Dali", "Baoshan", "Lijiang",
    "Dehong", "Pu'er", "Xishuangbanna", "Wenshan", "Pingxiang", "Fuyang", "Qinhuangdao", "Langfang", "Baotou",
    "Erdos", "Yulin", "Yan'an", "Xingtai", "Cangzhou", "Huangshi", "Xiangtan", "Loudi", "Zhangjiajie", "Yingkou"
]

BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

def fetch_weather_data(retries=3, delay=3):
    """
    Fetches weather data from OpenWeather API with retry logic.
    Handles API rate limits by adding a delay between requests.
    """
    weather_data = []

    for city in CITIES:
        for attempt in range(retries):
            try:
                params = {
                    "q": city,
                    "appid": API_KEY,
                    "units": "metric"
                }
                response = requests.get(BASE_URL, params=params, timeout=10)

                if response.status_code == 200:
                    data = response.json()
                    weather_data.append({
                        "city": data["name"],
                        "temperature": data["main"]["temp"],
                        "humidity": data["main"]["humidity"],
                        "weather": data["weather"][0]["description"],
                        "pressure": data["main"]["pressure"],
                        "wind_speed": data["wind"]["speed"],
                        "visibility": data.get("visibility", None),
                        "timestamp": data["dt"]
                    })
                    logging.info(f"Data fetched for {city}")
                    break  # Break out of retry loop on success
                else:
                    logging.warning(f"Attempt {attempt + 1} failed for {city}: {response.status_code}")
                    sleep(delay)  # Wait before retrying
            except requests.RequestException as e:
                logging.error(f"Request error for {city}: {e}")
                sleep(delay)  # Wait before retrying

        # **Wait 1 second between each API call** to prevent hitting rate limits
        sleep(1)

    if not weather_data:
        logging.error("No data was fetched from the API.")
    
    return pd.DataFrame(weather_data)

def save_data(df, file_path):
    """
    Saves DataFrame to CSV with a timestamped filename.
    """
    if df.empty:
        logging.warning("No data to save. Skipping file writing.")
        return
    
    output_folder = "data"
    os.makedirs(output_folder, exist_ok=True)  # Ensure folder exists

    filename = f"{output_folder}/weather_data_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(file_path, index=False)
    logging.info(f"Data saved successfully to {filename}")

if __name__ == "__main__":
    df = fetch_weather_data()
    save_data(df)
    logging.info("Weather data extraction complete!")
