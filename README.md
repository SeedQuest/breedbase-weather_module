# BreedBase Weather & GDD/CHU Module

Enhanced weather data integration for BreedBase with comprehensive Open-Meteo support.

## Features

### Weather Data Sources (Priority Order)
1. **Davis WeatherLink** - Ground-truth station data
2. **Ecowitt Cloud** - Local weather stations  
3. **Open-Meteo** - Satellite/reanalysis data (fallback, always available)

### Open-Meteo Daily Variables (17 fields)
| Category | Variables |
|----------|-----------|
| Temperature | max, min, mean |
| Precipitation | sum, rain, snowfall, hours |
| Solar | radiation (MJ/m²), sunshine duration |
| Atmospheric | humidity, dew point, ET₀ (FAO-56) |
| Wind | speed max, gusts, direction |
| Soil | temperature (0-7cm), moisture (m³/m³) |

### GDD/CHU Calculator
- Growing Degree Days (GDD) with configurable base temperature
- Crop Heat Units (CHU) - Ontario method
- Multi-year analysis support
- Excel export with all weather fields
- Interactive charts

## Installation

### 1. Copy files to your BreedBase custom directory:
```bash
cp Weather.pm /srv/breedbase/custom/lib/SGN/Controller/AJAX/
cp gdd_analysis.mas /srv/breedbase/custom/mason/breeders_toolbox/
```

### 2. Mount in docker-compose.yml:
```yaml
volumes:
  - /srv/breedbase/custom/lib/SGN/Controller/AJAX/Weather.pm:/home/production/cxgn/sgn/lib/SGN/Controller/AJAX/Weather.pm:ro
  - /srv/breedbase/custom/mason/breeders_toolbox/gdd_analysis.mas:/home/production/cxgn/sgn/mason/breeders_toolbox/gdd_analysis.mas:ro
```

### 3. Create database table:
```sql
CREATE TABLE IF NOT EXISTS weather_data (
    weather_data_id SERIAL PRIMARY KEY,
    location_id INTEGER NOT NULL REFERENCES nd_geolocation(nd_geolocation_id) ON DELETE CASCADE,
    date DATE NOT NULL,
    temp_max NUMERIC(5,2),
    temp_min NUMERIC(5,2),
    temp_mean NUMERIC(5,2),
    precipitation NUMERIC(6,2),
    humidity_mean NUMERIC(5,2),
    solar_radiation NUMERIC(8,2),
    evapotranspiration NUMERIC(5,2),
    wind_speed_max NUMERIC(5,2),
    dew_point NUMERIC(5,2),
    soil_temp NUMERIC(5,2),
    soil_moisture NUMERIC(6,4),
    source VARCHAR(50) DEFAULT 'open-meteo',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_id, date, source)
);
CREATE INDEX idx_weather_location_date ON weather_data(location_id, date);
```

### 4. Restart container:
```bash
docker restart breedbase_web
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/ajax/weather/gdd` | GET/POST | Calculate GDD/CHU for location |
| `/ajax/weather/crops` | GET | List available crop presets |
| `/ajax/weather/sources` | GET | List configured data sources |
| `/ajax/weather/cache/stats` | GET | Database cache statistics |

## Configuration (sgn_local.conf)

```perl
# Davis WeatherLink v2 (optional)
davis_api_key    YOUR_API_KEY
davis_api_secret YOUR_SECRET
davis_station_id YOUR_STATION_ID

# Ecowitt Cloud (optional)
ecowitt_app_key YOUR_APP_KEY
ecowitt_api_key YOUR_API_KEY
ecowitt_mac     YOUR_DEVICE_MAC
```

## License

MIT License - Free for commercial and non-commercial use.

## Author

SeedQuest (https://seedquest.com.ua)
