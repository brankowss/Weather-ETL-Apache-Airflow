CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(255),
    temperature FLOAT,
    humidity FLOAT,
    weather VARCHAR(255),
    pressure FLOAT,  
    wind_speed FLOAT,
    visibility FLOAT,  
    datetime TIMESTAMP,
    temperature_f FLOAT
);


