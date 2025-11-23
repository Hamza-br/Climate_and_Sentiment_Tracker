# Climate & Sentiment Tracker - API Documentation

## 🚀 Quick Start

Your API is running at: **http://localhost:8000**

Interactive documentation: **http://localhost:8000/docs**

## 📍 Available Endpoints

### 1. **Root Endpoint**
```
GET /
```
Returns API information and available endpoints.

**PowerShell Example:**
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/" -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json
```

**Response:**
```json
{
  "name": "Climate & Sentiment Tracker API",
  "version": "1.0.0",
  "status": "operational",
  "documentation": "/docs",
  "openapi": "/openapi.json",
  "endpoints": {
    "health": "/health",
    "weather": "/api/v1/weather/latest",
    "air_quality": "/api/v1/air-quality/latest",
    "sentiment": "/api/v1/sentiment/summary",
    "analytics": "/api/v1/analytics/daily"
  }
}
```

### 2. **Health Check**
```
GET /health
```
Check if the API and database connections are healthy.

**PowerShell Example:**
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/health" -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 3. **Latest Weather Data**
```
GET /api/v1/weather/latest?city=Boston&limit=10
```
Get the most recent weather data for a specific city.

**Parameters:**
- `city` (optional): City name (default: "Boston")
- `limit` (optional): Number of records to return (default: 10)

**PowerShell Example:**
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/v1/weather/latest?city=Boston&limit=5" -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json -Depth 5
```

### 4. **Latest Air Quality Data**
```
GET /api/v1/air-quality/latest?city=Boston&limit=10
```
Get the most recent air quality measurements.

**Parameters:**
- `city` (optional): City name (default: "Boston")
- `limit` (optional): Number of records to return (default: 10)

**PowerShell Example:**
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/v1/air-quality/latest?limit=5" -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json -Depth 5
```

### 5. **Sentiment Summary** ⭐
```
GET /api/v1/sentiment/summary
```
Get aggregated sentiment statistics from all social posts.

**PowerShell Example:**
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/v1/sentiment/summary" -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json
```

**Response:**
```json
{
  "total_posts": 2,
  "positive": 1,
  "negative": 1,
  "neutral": 0,
  "positive_pct": 50.0,
  "negative_pct": 50.0,
  "neutral_pct": 0.0,
  "avg_confidence": 0.93
}
```

### 6. **Daily Analytics**
```
GET /api/v1/analytics/daily
```
Get daily aggregated analytics.

**PowerShell Example:**
```powershell
Invoke-WebRequest -Uri "http://localhost:8000/api/v1/analytics/daily" -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json -Depth 5
```

## 🤖 AI Inference Service

Your sentiment analysis AI is running at: **http://localhost:8082**

### Predict Sentiment
```
POST /predict
Content-Type: application/json

{
  "text": "Your text here"
}
```

**PowerShell Example:**
```powershell
$body = @{text = "Climate change is a serious crisis!"} | ConvertTo-Json
Invoke-WebRequest -Uri "http://localhost:8082/predict" -Method POST -Body $body -ContentType "application/json" -UseBasicParsing | Select-Object -ExpandProperty Content
```

**Response:**
```json
{
  "sentiment": "Negative",
  "confidence": 0.82
}
```

## 🧪 Complete Test Script

Copy and run this PowerShell script to test all endpoints:

```powershell
# Test all Climate & Sentiment Tracker API endpoints

Write-Host "Testing Climate & Sentiment Tracker API..." -ForegroundColor Cyan

# 1. Root endpoint
Write-Host "`n1. Testing Root Endpoint..." -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:8000/" -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json

# 2. Health check
Write-Host "`n2. Testing Health Endpoint..." -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:8000/health" -UseBasicParsing | Select-Object -ExpandProperty Content

# 3. Sentiment summary
Write-Host "`n3. Testing Sentiment Summary..." -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:8000/api/v1/sentiment/summary" -UseBasicParsing | Select-Object -ExpandProperty Content | ConvertFrom-Json | ConvertTo-Json

# 4. Weather data
Write-Host "`n4. Testing Weather Endpoint..." -ForegroundColor Yellow
Invoke-WebRequest -Uri "http://localhost:8000/api/v1/weather/latest?limit=3" -UseBasicParsing | Select-Object -ExpandProperty Content

# 5. Inference service
Write-Host "`n5. Testing AI Sentiment Inference..." -ForegroundColor Yellow
$body = @{text = "The climate situation is terrible!"} | ConvertTo-Json
Invoke-WebRequest -Uri "http://localhost:8082/predict" -Method POST -Body $body -ContentType "application/json" -UseBasicParsing | Select-Object -ExpandProperty Content

Write-Host "`n✅ All tests completed!" -ForegroundColor Green
```

## 📊 Data Sources

The API retrieves data from:
- **Cassandra Database** (climate_sentiment keyspace)
  - `weather_data` table
  - `air_quality_data` table
  - `social_posts` table
  - `social_sentiment` table
  - `daily_aggregates` table

## 🔧 Troubleshooting

### API shows "unhealthy"
```powershell
# Check if Cassandra is running
docker exec cassandra cqlsh -e "SELECT COUNT(*) FROM climate_sentiment.weather_data;"

# Restart API
docker compose restart api
```

### No data in responses
```powershell
# Add sample data
docker exec cassandra cqlsh -e "INSERT INTO climate_sentiment.social_sentiment (post_id, event_time_utc, sentiment_label, sentiment_confidence_pct, body_normalized, source, keywords, ingestion_time) VALUES ('test1', toTimestamp(now()), 'Positive', 0.92, 'Great climate news!', 'x', ['climate'], toTimestamp(now()));"
```

### Check all containers
```powershell
docker compose ps
```

## 🎯 Production Checklist

- [ ] Add authentication/API keys
- [ ] Set up rate limiting
- [ ] Configure CORS for frontend
- [ ] Add request validation
- [ ] Set up monitoring/logging
- [ ] Configure SSL/HTTPS
- [ ] Add caching layer (Redis)
- [ ] Set up backup for Cassandra

## 📱 Integration Examples

### Python
```python
import requests

# Get sentiment summary
response = requests.get("http://localhost:8000/api/v1/sentiment/summary")
data = response.json()
print(f"Total posts: {data['total_posts']}")
print(f"Positive: {data['positive_pct']}%")
```

### JavaScript/Node.js
```javascript
fetch('http://localhost:8000/api/v1/sentiment/summary')
  .then(response => response.json())
  .then(data => console.log(data));
```

### cURL
```bash
curl http://localhost:8000/api/v1/sentiment/summary
```

---

**Your API is ready for demo!** 🎉

All endpoints are operational and returning real-time data from your Climate & Sentiment Tracker pipeline.
