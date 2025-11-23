#!/usr/bin/env python3
"""
FastAPI application for Climate & Sentiment Tracker
Exposes Cassandra data for visualization and analytics
"""

import os
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uvicorn

# ============================================================
# Logging Configuration
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# Environment Configuration
# ============================================================
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "climate_sentiment")
CASSANDRA_USER = os.getenv("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "cassandra")

# ============================================================
# Pydantic Models (Response DTOs)
# ============================================================

class WeatherData(BaseModel):
    city: str
    event_time_utc: datetime
    temperature_celsius: float
    feels_like_temp: float
    humidity: float
    precipitation: float
    wind_speed: float
    wind_category: str
    weather_anomaly: str

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class AirQualityData(BaseModel):
    city: str
    event_time_utc: datetime
    parameter_normalized: str
    value: float
    unit: str
    aqi_category: str
    aqi_score: float
    health_risk_level: int
    pollution_spike: bool

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class SocialPostData(BaseModel):
    post_id: str
    event_time_utc: datetime
    title_normalized: str
    body_normalized: str
    author_normalized: str
    source: str
    upvotes: int
    keywords: List[str]
    regions_detected: List[str]

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class SocialSentimentData(BaseModel):
    post_id: str
    event_time_utc: datetime
    sentiment_label: str
    sentiment_category: str
    sentiment_confidence_pct: float
    engagement_score: float

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class DailyAggregateData(BaseModel):
    date: str
    city: str
    avg_temperature: Optional[float]
    avg_feels_like: Optional[float]
    max_temperature: Optional[float]
    min_temperature: Optional[float]
    avg_humidity: Optional[float]
    total_precipitation: Optional[float]
    avg_wind_speed: Optional[float]
    avg_aqi_score: Optional[float]
    avg_health_risk: Optional[float]
    num_social_posts: int
    avg_sentiment_confidence: Optional[float]
    positive_posts: int
    negative_posts: int
    neutral_posts: int
    anomaly_count: int
    pollution_spike_count: int

class HealthStatus(BaseModel):
    status: str
    cassandra: str
    tables: Dict[str, int]
    timestamp: datetime

class SentimentSummary(BaseModel):
    total_posts: int
    positive: int
    negative: int
    neutral: int
    positive_pct: float
    negative_pct: float
    neutral_pct: float
    avg_confidence: float

class CorrelationMetrics(BaseModel):
    sentiment_weather_strong: int
    sentiment_weather_moderate: int
    sentiment_weather_weak: int
    sentiment_pollution_strong: int
    sentiment_pollution_moderate: int
    sentiment_pollution_weak: int

# ============================================================
# Cassandra Connection Manager
# ============================================================

class CassandraConnection:
    def __init__(self):
        self.cluster = None
        self.session = None

    def connect(self):
        """Connect to Cassandra cluster"""
        try:
            auth_provider = PlainTextAuthProvider(
                username=CASSANDRA_USER,
                password=CASSANDRA_PASSWORD
            )
            self.cluster = Cluster(
                [CASSANDRA_HOST],
                port=CASSANDRA_PORT,
                auth_provider=auth_provider
            )
            self.session = self.cluster.connect(CASSANDRA_KEYSPACE)
            logger.info(f"✓ Connected to Cassandra: {CASSANDRA_HOST}:{CASSANDRA_PORT}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to connect to Cassandra: {str(e)}")
            return False

    def disconnect(self):
        """Disconnect from Cassandra"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        logger.info("✓ Disconnected from Cassandra")

    def execute(self, query: str, params: List = None):
        """Execute CQL query"""
        if not self.session:
            raise Exception("Not connected to Cassandra")
        if params:
            return self.session.execute(query, params)
        return self.session.execute(query)

# ============================================================
# FastAPI Application Setup
# ============================================================

db = CassandraConnection()

async def lifespan(app: FastAPI):
    """Manage application lifespan"""
    logger.info("=" * 60)
    logger.info("CLIMATE & SENTIMENT TRACKER API - STARTUP")
    logger.info("=" * 60)
    if not db.connect():
        logger.error("Failed to initialize Cassandra connection")
        raise Exception("Cannot connect to Cassandra")
    logger.info("✓ API startup complete")
    yield
    logger.info("API shutting down...")
    db.disconnect()

app = FastAPI(
    title="Climate & Sentiment Tracker API",
    description="REST API for Boston climate and sentiment data from Cassandra",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# Health Check Endpoint
# ============================================================

@app.get("/health", response_model=HealthStatus)
async def health_check():
    """Health check endpoint with table statistics"""
    try:
        tables = {}
        for table in ["weather_data", "air_quality_data", "social_posts", "social_sentiment", "daily_aggregates"]:
            try:
                result = db.execute(f"SELECT COUNT(*) as count FROM {table}")
                row = result.one()
                tables[table] = row.count if row else 0
            except Exception as e:
                logger.warning(f"Could not count {table}: {str(e)}")
                tables[table] = 0
        return HealthStatus(
            status="healthy",
            cassandra="connected",
            tables=tables,
            timestamp=datetime.utcnow()
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

# ============================================================
# Weather Endpoints
# ============================================================

@app.get("/api/v1/weather/latest", response_model=List[WeatherData])
async def get_latest_weather(
    limit: int = Query(10, ge=1, le=100),
    anomaly_only: bool = Query(False)
):
    """Get latest weather data"""
    try:
        query = "SELECT * FROM weather_data WHERE city='Boston, USA' ORDER BY event_time_utc DESC LIMIT %s"
        result = db.execute(query, [limit])
        rows = result.all()
        data = []
        for row in rows:
            if anomaly_only and row.weather_anomaly == "Normal":
                continue
            data.append(WeatherData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                temperature_celsius=row.temperature_celsius,
                feels_like_temp=row.feels_like_temp,
                humidity=row.humidity,
                precipitation=row.precipitation,
                wind_speed=row.wind_speed,
                wind_category=row.wind_category,
                weather_anomaly=row.weather_anomaly
            ))
        return data
    except Exception as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/weather/anomalies", response_model=List[WeatherData])
async def get_weather_anomalies(limit: int = Query(50, ge=1, le=500)):
    """Get anomalous weather events"""
    try:
        # Cannot use != in CQL, so we fetch recent data and filter in Python
        query = """
        SELECT * FROM weather_data 
        WHERE city='Boston, USA'
        ORDER BY event_time_utc DESC
        LIMIT %s
        """
        # Fetch 3x limit to increase chance of finding anomalies
        result = db.execute(query, [limit * 3])
        rows = result.all()
        
        anomalies = []
        for row in rows:
            if row.weather_anomaly != 'Normal':
                anomalies.append(WeatherData(
                    city=row.city,
                    event_time_utc=row.event_time_utc,
                    temperature_celsius=row.temperature_celsius,
                    feels_like_temp=row.feels_like_temp,
                    humidity=row.humidity,
                    precipitation=row.precipitation,
                    wind_speed=row.wind_speed,
                    wind_category=row.wind_category,
                    weather_anomaly=row.weather_anomaly
                ))
                if len(anomalies) >= limit:
                    break
        return anomalies
    except Exception as e:
        logger.error(f"Error fetching weather anomalies: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Air Quality Endpoints
# ============================================================

@app.get("/api/v1/air-quality/latest", response_model=List[AirQualityData])
async def get_latest_air_quality(
    limit: int = Query(10, ge=1, le=100),
    parameter: Optional[str] = Query(None),
    spike_only: bool = Query(False)
):
    """Get latest air quality data"""
    try:
        if parameter:
            query = """
            SELECT * FROM air_quality_data 
            WHERE city='Boston, USA' AND parameter_normalized=%s
            ORDER BY event_time_utc DESC
            LIMIT %s
            """
            result = db.execute(query, [parameter, limit])
        else:
            query = """
            SELECT * FROM air_quality_data 
            WHERE city='Boston, USA'
            ORDER BY event_time_utc DESC
            LIMIT %s
            """
            result = db.execute(query, [limit])
        rows = result.all()
        data = []
        for row in rows:
            if spike_only and not row.pollution_spike:
                continue
            data.append(AirQualityData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                parameter_normalized=row.parameter_normalized,
                value=row.value,
                unit=row.unit,
                aqi_category=row.aqi_category,
                aqi_score=row.aqi_score,
                health_risk_level=row.health_risk_level,
                pollution_spike=row.pollution_spike
            ))
        return data
    except Exception as e:
        logger.error(f"Error fetching air quality data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/air-quality/pollution-spikes", response_model=List[AirQualityData])
async def get_pollution_spikes(limit: int = Query(50, ge=1, le=500)):
    """Get pollution spike events"""
    try:
        # Added ALLOW FILTERING as pollution_spike is not indexed
        query = """
        SELECT * FROM air_quality_data 
        WHERE city='Boston, USA' AND pollution_spike=true
        ORDER BY event_time_utc DESC
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit])
        rows = result.all()
        return [
            AirQualityData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                parameter_normalized=row.parameter_normalized,
                value=row.value,
                unit=row.unit,
                aqi_category=row.aqi_category,
                aqi_score=row.aqi_score,
                health_risk_level=row.health_risk_level,
                pollution_spike=row.pollution_spike
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching pollution spikes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Social Posts Endpoints
# ============================================================

@app.get("/api/v1/social/posts/latest", response_model=List[SocialPostData])
async def get_latest_posts(
    limit: int = Query(20, ge=1, le=100),
    source: Optional[str] = Query(None)
):
    """Get latest social posts"""
    try:
        # Removed ORDER BY as we cannot order globally without partition key
        # Fetching more rows to sort in Python
        query = """
        SELECT * FROM social_posts 
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit * 5])
        rows = result.all()
        
        # Sort in Python
        sorted_rows = sorted(rows, key=lambda x: x.event_time_utc, reverse=True)
        
        data = []
        for row in sorted_rows:
            if source and row.source != source:
                continue
            data.append(SocialPostData(
                post_id=row.post_id,
                event_time_utc=row.event_time_utc,
                title_normalized=row.title_normalized or "",
                body_normalized=row.body_normalized or "",
                author_normalized=row.author_normalized or "",
                source=row.source,
                upvotes=row.upvotes or 0,
                keywords=list(row.keywords) if row.keywords else [],
                regions_detected=list(row.regions_detected) if row.regions_detected else []
            ))
            if len(data) >= limit:
                break
        return data
    except Exception as e:
        logger.error(f"Error fetching social posts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Sentiment Analysis Endpoints
# ============================================================

@app.get("/api/v1/sentiment/latest", response_model=List[SocialSentimentData])
async def get_latest_sentiment(
    limit: int = Query(20, ge=1, le=100),
    label: Optional[str] = Query(None)
):
    """Get latest sentiment predictions"""
    try:
        if label and label.lower() in ["positive", "negative", "neutral"]:
            # Cannot ORDER BY with secondary index unless partition key is restricted
            query = """
            SELECT * FROM social_sentiment 
            WHERE sentiment_label=%s
            LIMIT %s
            """
            result = db.execute(query, [label.lower(), limit * 5])
        else:
            query = """
            SELECT * FROM social_sentiment 
            LIMIT %s
            ALLOW FILTERING
            """
            result = db.execute(query, [limit * 5])
            
        rows = result.all()
        # Sort in Python
        sorted_rows = sorted(rows, key=lambda x: x.event_time_utc, reverse=True)[:limit]
        
        return [
            SocialSentimentData(
                post_id=row.post_id,
                event_time_utc=row.event_time_utc,
                sentiment_label=row.sentiment_label,
                sentiment_category=row.sentiment_category or "neutral",
                sentiment_confidence_pct=row.sentiment_confidence_pct or 0.0,
                engagement_score=row.engagement_score or 0.0
            )
            for row in sorted_rows
        ]
    except Exception as e:
        logger.error(f"Error fetching latest sentiment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/sentiment/summary", response_model=SentimentSummary)
async def get_sentiment_summary(
    days: int = Query(1, ge=1, le=90)
):
    """Get sentiment summary for the last N days"""
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        query = """
        SELECT * FROM social_sentiment
        WHERE event_time_utc >= %s
        ALLOW FILTERING
        """
        rows = db.execute(query, [cutoff]).all()
        positive = negative = neutral = 0
        total_confidence = 0.0
        count = 0
        for row in rows:
            label = (row.sentiment_label or "neutral").lower()
            if label == "positive":
                positive += 1
            elif label == "negative":
                negative += 1
            else:
                neutral += 1
            total_confidence += row.sentiment_confidence_pct or 0.0
            count += 1
        total = positive + negative + neutral
        return SentimentSummary(
            total_posts=total,
            positive=positive,
            negative=negative,
            neutral=neutral,
            positive_pct=round((positive / total * 100) if total else 0, 2),
            negative_pct=round((negative / total * 100) if total else 0, 2),
            neutral_pct=round((neutral / total * 100) if total else 0, 2),
            avg_confidence=round(total_confidence / count if count else 0, 2)
        )
    except Exception as e:
        logger.error(f"Error calculating sentiment summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Analytics Endpoints
# ============================================================

@app.get("/api/v1/analytics/daily", response_model=List[DailyAggregateData])
async def get_daily_aggregates(
    days: int = Query(30, ge=1, le=90)
):
    """Get daily aggregated metrics"""
    try:
        query = """
        SELECT * FROM daily_aggregates 
        WHERE city='Boston, USA'
        ORDER BY date DESC
        LIMIT %s
        """
        result = db.execute(query, [days])
        rows = result.all()
        return [
            DailyAggregateData(
                date=str(row.date),
                city=row.city,
                avg_temperature=row.avg_temperature,
                avg_feels_like=row.avg_feels_like,
                max_temperature=row.max_temperature,
                min_temperature=row.min_temperature,
                avg_humidity=row.avg_humidity,
                total_precipitation=row.total_precipitation,
                avg_wind_speed=row.avg_wind_speed,
                avg_aqi_score=row.avg_aqi_score,
                avg_health_risk=row.avg_health_risk,
                num_social_posts=row.num_social_posts,
                avg_sentiment_confidence=row.avg_sentiment_confidence,
                positive_posts=row.positive_posts,
                negative_posts=row.negative_posts,
                neutral_posts=row.neutral_posts,
                anomaly_count=row.anomaly_count,
                pollution_spike_count=row.pollution_spike_count
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching daily aggregates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analytics/correlation", response_model=CorrelationMetrics)
async def get_correlation_metrics(days: int = Query(30, ge=1, le=90)):
    """Get sentiment-environmental correlation metrics"""
    try:
        query = """
        SELECT * FROM daily_aggregates 
        WHERE city='Boston, USA'
        ORDER BY date DESC
        LIMIT %s
        """
        result = db.execute(query, [days])
        rows = result.all()
        strong_weather = sum(getattr(r, 'strong_sentiment_weather_correlation', 0) for r in rows)
        strong_pollution = sum(getattr(r, 'strong_sentiment_pollution_correlation', 0) for r in rows)
        return CorrelationMetrics(
            sentiment_weather_strong=strong_weather,
            sentiment_weather_moderate=0,
            sentiment_weather_weak=0,
            sentiment_pollution_strong=strong_pollution,
            sentiment_pollution_moderate=0,
            sentiment_pollution_weak=0
        )
    except Exception as e:
        logger.error(f"Error fetching correlation metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Search & Filter Endpoints
# ============================================================

@app.get("/api/v1/search/keywords")
async def search_by_keywords(
    keywords: str = Query(..., description="Comma-separated keywords"),
    limit: int = Query(20, ge=1, le=100)
):
    """Search social posts by keywords"""
    try:
        keyword_list = [k.strip().lower() for k in keywords.split(",")]
        # Removed ORDER BY
        query = """
        SELECT * FROM social_posts 
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit * 5])
        rows = result.all()
        
        # Sort in Python
        sorted_rows = sorted(rows, key=lambda x: x.event_time_utc, reverse=True)
        
        filtered = []
        for row in sorted_rows:
            row_keywords = [k.lower() for k in (row.keywords or [])]
            if any(kw in row_keywords for kw in keyword_list):
                filtered.append(SocialPostData(
                    post_id=row.post_id,
                    event_time_utc=row.event_time_utc,
                    title_normalized=row.title_normalized or "",
                    body_normalized=row.body_normalized or "",
                    author_normalized=row.author_normalized or "",
                    source=row.source,
                    upvotes=row.upvotes or 0,
                    keywords=list(row.keywords) if row.keywords else [],
                    regions_detected=list(row.regions_detected) if row.regions_detected else []
                ))
                if len(filtered) >= limit:
                    break
        return filtered
    except Exception as e:
        logger.error(f"Error searching keywords: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/search/regions")
async def search_by_region(
    region: str = Query(..., description="Boston neighborhood"),
    limit: int = Query(20, ge=1, le=100)
):
    """Search posts by Boston region/neighborhood"""
    try:
        # Removed ORDER BY
        query = """
        SELECT * FROM social_posts 
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit * 5])
        rows = result.all()
        
        # Sort in Python
        sorted_rows = sorted(rows, key=lambda x: x.event_time_utc, reverse=True)
        
        filtered = []
        for row in sorted_rows:
            regions = [r.lower() for r in (row.regions_detected or [])]
            if region.lower() in regions:
                filtered.append(SocialPostData(
                    post_id=row.post_id,
                    event_time_utc=row.event_time_utc,
                    title_normalized=row.title_normalized or "",
                    body_normalized=row.body_normalized or "",
                    author_normalized=row.author_normalized or "",
                    source=row.source,
                    upvotes=row.upvotes or 0,
                    keywords=list(row.keywords) if row.keywords else [],
                    regions_detected=list(row.regions_detected) if row.regions_detected else []
                ))
                if len(filtered) >= limit:
                    break
        return filtered
    except Exception as e:
        logger.error(f"Error searching regions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Root Endpoint
# ============================================================

@app.get("/")
async def root():
    """Root endpoint with API documentation"""
    return {
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

# ============================================================
# Main Entry Point
# ============================================================

if __name__ == "__main__":
    logger.info("Starting Climate & Sentiment Tracker API...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
"""
FastAPI application for Climate & Sentiment Tracker
Exposes Cassandra data for visualization and analytics
"""

import os
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uvicorn

# ============================================================
# Logging Configuration
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# Environment Configuration
# ============================================================
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "climate_sentiment")
CASSANDRA_USER = os.getenv("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "cassandra")

# ============================================================
# Pydantic Models (Response DTOs)
# ============================================================

class WeatherData(BaseModel):
    city: str
    event_time_utc: datetime
    temperature_celsius: float
    feels_like_temp: float
    humidity: float
    precipitation: float
    wind_speed: float
    wind_category: str
    weather_anomaly: str

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class AirQualityData(BaseModel):
    city: str
    event_time_utc: datetime
    parameter_normalized: str
    value: float
    unit: str
    aqi_category: str
    aqi_score: float
    health_risk_level: int
    pollution_spike: bool

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class SocialPostData(BaseModel):
    post_id: str
    event_time_utc: datetime
    title_normalized: str
    body_normalized: str
    author_normalized: str
    source: str
    upvotes: int
    keywords: List[str]
    regions_detected: List[str]

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class SocialSentimentData(BaseModel):
    post_id: str
    event_time_utc: datetime
    sentiment_label: str
    sentiment_category: str
    sentiment_confidence_pct: float
    engagement_score: float

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class DailyAggregateData(BaseModel):
    date: str
    city: str
    avg_temperature: Optional[float]
    avg_feels_like: Optional[float]
    max_temperature: Optional[float]
    min_temperature: Optional[float]
    avg_humidity: Optional[float]
    total_precipitation: Optional[float]
    avg_wind_speed: Optional[float]
    avg_aqi_score: Optional[float]
    avg_health_risk: Optional[float]
    num_social_posts: int
    avg_sentiment_confidence: Optional[float]
    positive_posts: int
    negative_posts: int
    neutral_posts: int
    anomaly_count: int
    pollution_spike_count: int

class HealthStatus(BaseModel):
    status: str
    cassandra: str
    tables: Dict[str, int]
    timestamp: datetime

class SentimentSummary(BaseModel):
    total_posts: int
    positive: int
    negative: int
    neutral: int
    positive_pct: float
    negative_pct: float
    neutral_pct: float
    avg_confidence: float

class CorrelationMetrics(BaseModel):
    sentiment_weather_strong: int
    sentiment_weather_moderate: int
    sentiment_weather_weak: int
    sentiment_pollution_strong: int
    sentiment_pollution_moderate: int
    sentiment_pollution_weak: int

# ============================================================
# Cassandra Connection Manager
# ============================================================

class CassandraConnection:
    def __init__(self):
        self.cluster = None
        self.session = None

    def connect(self):
        """Connect to Cassandra cluster"""
        try:
            auth_provider = PlainTextAuthProvider(
                username=CASSANDRA_USER,
                password=CASSANDRA_PASSWORD
            )
            self.cluster = Cluster(
                [CASSANDRA_HOST],
                port=CASSANDRA_PORT,
                auth_provider=auth_provider
            )
            self.session = self.cluster.connect(CASSANDRA_KEYSPACE)
            logger.info(f"✓ Connected to Cassandra: {CASSANDRA_HOST}:{CASSANDRA_PORT}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to connect to Cassandra: {str(e)}")
            return False

    def disconnect(self):
        """Disconnect from Cassandra"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        logger.info("✓ Disconnected from Cassandra")

    def execute(self, query: str, params: List = None):
        """Execute CQL query"""
        if not self.session:
            raise Exception("Not connected to Cassandra")
        if params:
            return self.session.execute(query, params)
        return self.session.execute(query)

# ============================================================
# FastAPI Application Setup
# ============================================================

db = CassandraConnection()

async def lifespan(app: FastAPI):
    """Manage application lifespan"""
    logger.info("=" * 60)
    logger.info("CLIMATE & SENTIMENT TRACKER API - STARTUP")
    logger.info("=" * 60)
    if not db.connect():
        logger.error("Failed to initialize Cassandra connection")
        raise Exception("Cannot connect to Cassandra")
    logger.info("✓ API startup complete")
    yield
    logger.info("API shutting down...")
    db.disconnect()

app = FastAPI(
    title="Climate & Sentiment Tracker API",
    description="REST API for Boston climate and sentiment data from Cassandra",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# Health Check Endpoint
# ============================================================

@app.get("/health", response_model=HealthStatus)
async def health_check():
    """Health check endpoint with table statistics"""
    try:
        tables = {}
        for table in ["weather_data", "air_quality_data", "social_posts", "social_sentiment", "daily_aggregates"]:
            try:
                result = db.execute(f"SELECT COUNT(*) as count FROM {table}")
                row = result.one()
                tables[table] = row.count if row else 0
            except Exception as e:
                logger.warning(f"Could not count {table}: {str(e)}")
                tables[table] = 0
        return HealthStatus(
            status="healthy",
            cassandra="connected",
            tables=tables,
            timestamp=datetime.utcnow()
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

# ============================================================
# Weather Endpoints
# ============================================================

@app.get("/api/v1/weather/latest", response_model=List[WeatherData])
async def get_latest_weather(
    limit: int = Query(10, ge=1, le=100),
    anomaly_only: bool = Query(False)
):
    """Get latest weather data"""
    try:
        query = "SELECT * FROM weather_data WHERE city='Boston, USA' ORDER BY event_time_utc DESC LIMIT %s"
        result = db.execute(query, [limit])
        rows = result.all()
        data = []
        for row in rows:
            if anomaly_only and row.weather_anomaly == "Normal":
                continue
            data.append(WeatherData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                temperature_celsius=row.temperature_celsius,
                feels_like_temp=row.feels_like_temp,
                humidity=row.humidity,
                precipitation=row.precipitation,
                wind_speed=row.wind_speed,
                wind_category=row.wind_category,
                weather_anomaly=row.weather_anomaly
            ))
        return data
    except Exception as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/weather/anomalies", response_model=List[WeatherData])
async def get_weather_anomalies(limit: int = Query(50, ge=1, le=500)):
    """Get anomalous weather events"""
    try:
        # Cannot use != in CQL, so we fetch recent data and filter in Python
        query = """
        SELECT * FROM weather_data 
        WHERE city='Boston, USA'
        ORDER BY event_time_utc DESC
        LIMIT %s
        """
        # Fetch 3x limit to increase chance of finding anomalies
        result = db.execute(query, [limit * 3])
        rows = result.all()
        
        anomalies = []
        for row in rows:
            if row.weather_anomaly != 'Normal':
                anomalies.append(WeatherData(
                    city=row.city,
                    event_time_utc=row.event_time_utc,
                    temperature_celsius=row.temperature_celsius,
                    feels_like_temp=row.feels_like_temp,
                    humidity=row.humidity,
                    precipitation=row.precipitation,
                    wind_speed=row.wind_speed,
                    wind_category=row.wind_category,
                    weather_anomaly=row.weather_anomaly
                ))
                if len(anomalies) >= limit:
                    break
        return anomalies
    except Exception as e:
        logger.error(f"Error fetching weather anomalies: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Air Quality Endpoints
# ============================================================

@app.get("/api/v1/air-quality/latest", response_model=List[AirQualityData])
async def get_latest_air_quality(
    limit: int = Query(10, ge=1, le=100),
    parameter: Optional[str] = Query(None),
    spike_only: bool = Query(False)
):
    """Get latest air quality data"""
    try:
        if parameter:
            query = """
            SELECT * FROM air_quality_data 
            WHERE city='Boston, USA' AND parameter_normalized=%s
            ORDER BY event_time_utc DESC
            LIMIT %s
            """
            result = db.execute(query, [parameter, limit])
        else:
            query = """
            SELECT * FROM air_quality_data 
            WHERE city='Boston, USA'
            ORDER BY event_time_utc DESC
            LIMIT %s
            """
            result = db.execute(query, [limit])
        rows = result.all()
        data = []
        for row in rows:
            if spike_only and not row.pollution_spike:
                continue
            data.append(AirQualityData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                parameter_normalized=row.parameter_normalized,
                value=row.value,
                unit=row.unit,
                aqi_category=row.aqi_category,
                aqi_score=row.aqi_score,
                health_risk_level=row.health_risk_level,
                pollution_spike=row.pollution_spike
            ))
        return data
    except Exception as e:
        logger.error(f"Error fetching air quality data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/air-quality/pollution-spikes", response_model=List[AirQualityData])
async def get_pollution_spikes(limit: int = Query(50, ge=1, le=500)):
    """Get pollution spike events"""
    try:
        # Added ALLOW FILTERING as pollution_spike is not indexed
        query = """
        SELECT * FROM air_quality_data 
        WHERE city='Boston, USA' AND pollution_spike=true
        ORDER BY event_time_utc DESC
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit])
        rows = result.all()
        return [
            AirQualityData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                parameter_normalized=row.parameter_normalized,
                value=row.value,
                unit=row.unit,
                aqi_category=row.aqi_category,
                aqi_score=row.aqi_score,
                health_risk_level=row.health_risk_level,
                pollution_spike=row.pollution_spike
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching pollution spikes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Social Posts Endpoints
# ============================================================

@app.get("/api/v1/social/posts/latest", response_model=List[SocialPostData])
async def get_latest_posts(
    limit: int = Query(20, ge=1, le=100),
    source: Optional[str] = Query(None)
):
    """Get latest social posts"""
    try:
        # Removed ORDER BY as we cannot order globally without partition key
        # Fetching more rows to sort in Python
        query = """
        SELECT * FROM social_posts 
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit * 5])
        rows = result.all()
        
        # Sort in Python
        sorted_rows = sorted(rows, key=lambda x: x.event_time_utc, reverse=True)
        
        data = []
        for row in sorted_rows:
            if source and row.source != source:
                continue
            data.append(SocialPostData(
                post_id=row.post_id,
                event_time_utc=row.event_time_utc,
                title_normalized=row.title_normalized,
                body_normalized=row.body_normalized,
                author_normalized=row.author_normalized,
                source=row.source,
                upvotes=row.upvotes,
                keywords=list(row.keywords) if row.keywords else [],
                regions_detected=list(row.regions_detected) if row.regions_detected else []
            ))
            if len(data) >= limit:
                break
        return data
    except Exception as e:
        logger.error(f"Error fetching social posts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Sentiment Analysis Endpoints
# ============================================================

@app.get("/api/v1/sentiment/latest", response_model=List[SocialSentimentData])
async def get_latest_sentiment(
    limit: int = Query(20, ge=1, le=100),
    label: Optional[str] = Query(None)
):
    """Get latest sentiment predictions"""
    try:
        if label and label.lower() in ["positive", "negative", "neutral"]:
            # Cannot ORDER BY with secondary index unless partition key is restricted
            query = """
            SELECT * FROM social_sentiment 
            WHERE sentiment_label=%s
            LIMIT %s
            """
            result = db.execute(query, [label.lower(), limit * 5])
        else:
            query = """
            SELECT * FROM social_sentiment 
            LIMIT %s
            ALLOW FILTERING
            """
            result = db.execute(query, [limit * 5])
            
        rows = result.all()
        # Sort in Python
        sorted_rows = sorted(rows, key=lambda x: x.event_time_utc, reverse=True)[:limit]
        
        return [
            SocialSentimentData(
                post_id=row.post_id,
                event_time_utc=row.event_time_utc,
                sentiment_label=row.sentiment_label,
                sentiment_category=row.sentiment_category,
                sentiment_confidence_pct=row.sentiment_confidence_pct,
                engagement_score=row.engagement_score
            )
            for row in sorted_rows
        ]
    except Exception as e:
        logger.error(f"Error fetching latest sentiment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/sentiment/summary", response_model=SentimentSummary)
async def get_sentiment_summary(
    days: int = Query(1, ge=1, le=90)
):
    """Get sentiment summary for the last N days"""
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        query = """
        SELECT * FROM social_sentiment
        WHERE event_time_utc >= %s
        ALLOW FILTERING
        """
        rows = db.execute(query, [cutoff]).all()
        positive = negative = neutral = 0
        total_confidence = 0.0
        count = 0
        for row in rows:
            label = (row.sentiment_label or "neutral").lower()
            if label == "positive":
                positive += 1
            elif label == "negative":
                negative += 1
            else:
                neutral += 1
            total_confidence += row.sentiment_confidence_pct or 0.0
            count += 1
        total = positive + negative + neutral
        return SentimentSummary(
            total_posts=total,
            positive=positive,
            negative=negative,
            neutral=neutral,
            positive_pct=round((positive / total * 100) if total else 0, 2),
            negative_pct=round((negative / total * 100) if total else 0, 2),
            neutral_pct=round((neutral / total * 100) if total else 0, 2),
            avg_confidence=round(total_confidence / count if count else 0, 2)
        )
    except Exception as e:
        logger.error(f"Error calculating sentiment summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Analytics Endpoints
# ============================================================

@app.get("/api/v1/analytics/daily", response_model=List[DailyAggregateData])
async def get_daily_aggregates(
    days: int = Query(30, ge=1, le=90)
):
    """Get daily aggregated metrics"""
    try:
        query = """
        SELECT * FROM daily_aggregates 
        WHERE city='Boston, USA'
        ORDER BY date DESC
        LIMIT %s
        """
        result = db.execute(query, [days])
        rows = result.all()
        return [
            DailyAggregateData(
                date=str(row.date),
                city=row.city,
                avg_temperature=row.avg_temperature,
                avg_feels_like=row.avg_feels_like,
                max_temperature=row.max_temperature,
                min_temperature=row.min_temperature,
                avg_humidity=row.avg_humidity,
                total_precipitation=row.total_precipitation,
                avg_wind_speed=row.avg_wind_speed,
                avg_aqi_score=row.avg_aqi_score,
                avg_health_risk=row.avg_health_risk,
                num_social_posts=row.num_social_posts,
                avg_sentiment_confidence=row.avg_sentiment_confidence,
                positive_posts=row.positive_posts,
                negative_posts=row.negative_posts,
                neutral_posts=row.neutral_posts,
                anomaly_count=row.anomaly_count,
                pollution_spike_count=row.pollution_spike_count
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching daily aggregates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analytics/correlation", response_model=CorrelationMetrics)
async def get_correlation_metrics(days: int = Query(30, ge=1, le=90)):
    """Get sentiment-environmental correlation metrics"""
    try:
        query = """
        SELECT * FROM daily_aggregates 
        WHERE city='Boston, USA'
        ORDER BY date DESC
        LIMIT %s
        """
        result = db.execute(query, [days])
        rows = result.all()
        strong_weather = sum(getattr(r, 'strong_sentiment_weather_correlation', 0) for r in rows)
        strong_pollution = sum(getattr(r, 'strong_sentiment_pollution_correlation', 0) for r in rows)
        return CorrelationMetrics(
            sentiment_weather_strong=strong_weather,
            sentiment_weather_moderate=0,
            sentiment_weather_weak=0,
            sentiment_pollution_strong=strong_pollution,
            sentiment_pollution_moderate=0,
            sentiment_pollution_weak=0
        )
    except Exception as e:
        logger.error(f"Error fetching correlation metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Search & Filter Endpoints
# ============================================================

@app.get("/api/v1/search/keywords")
async def search_by_keywords(
    keywords: str = Query(..., description="Comma-separated keywords"),
    limit: int = Query(20, ge=1, le=100)
):
    """Search social posts by keywords"""
    try:
        keyword_list = [k.strip().lower() for k in keywords.split(",")]
        # Removed ORDER BY
        query = """
        SELECT * FROM social_posts 
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit * 5])
        rows = result.all()
        
        # Sort in Python
        sorted_rows = sorted(rows, key=lambda x: x.event_time_utc, reverse=True)
        
        filtered = []
        for row in sorted_rows:
            row_keywords = [k.lower() for k in (row.keywords or [])]
            if any(kw in row_keywords for kw in keyword_list):
                filtered.append(SocialPostData(
                    post_id=row.post_id,
                    event_time_utc=row.event_time_utc,
                    title_normalized=row.title_normalized,
                    body_normalized=row.body_normalized,
                    author_normalized=row.author_normalized,
                    source=row.source,
                    upvotes=row.upvotes,
                    keywords=list(row.keywords) if row.keywords else [],
                    regions_detected=list(row.regions_detected) if row.regions_detected else []
                ))
                if len(filtered) >= limit:
                    break
        return filtered
    except Exception as e:
        logger.error(f"Error searching keywords: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/search/regions")
async def search_by_region(
    region: str = Query(..., description="Boston neighborhood"),
    limit: int = Query(20, ge=1, le=100)
):
    """Search posts by Boston region/neighborhood"""
    try:
        # Removed ORDER BY
        query = """
        SELECT * FROM social_posts 
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit * 5])
        rows = result.all()
        
        # Sort in Python
        sorted_rows = sorted(rows, key=lambda x: x.event_time_utc, reverse=True)
        
        filtered = []
        for row in sorted_rows:
            regions = [r.lower() for r in (row.regions_detected or [])]
            if region.lower() in regions:
                filtered.append(SocialPostData(
                    post_id=row.post_id,
                    event_time_utc=row.event_time_utc,
                    title_normalized=row.title_normalized,
                    body_normalized=row.body_normalized,
                    author_normalized=row.author_normalized,
                    source=row.source,
                    upvotes=row.upvotes,
                    keywords=list(row.keywords) if row.keywords else [],
                    regions_detected=list(row.regions_detected) if row.regions_detected else []
                ))
                if len(filtered) >= limit:
                    break
        return filtered
    except Exception as e:
        logger.error(f"Error searching regions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Root Endpoint
# ============================================================

@app.get("/")
async def root():
    """Root endpoint with API documentation"""
    return {
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

# ============================================================
# Main Entry Point
# ============================================================

if __name__ == "__main__":
    logger.info("Starting Climate & Sentiment Tracker API...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )
"""
FastAPI application for Climate & Sentiment Tracker
Exposes Cassandra data for visualization and analytics
"""

import os
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uvicorn

# ============================================================
# Logging Configuration
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# Environment Configuration
# ============================================================
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "climate_sentiment")
CASSANDRA_USER = os.getenv("CASSANDRA_USER", "cassandra")
CASSANDRA_PASSWORD = os.getenv("CASSANDRA_PASSWORD", "cassandra")

# ============================================================
# Pydantic Models (Response DTOs)
# ============================================================

class WeatherData(BaseModel):
    city: str
    event_time_utc: datetime
    temperature_celsius: float
    feels_like_temp: float
    humidity: float
    precipitation: float
    wind_speed: float
    wind_category: str
    weather_anomaly: str

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class AirQualityData(BaseModel):
    city: str
    event_time_utc: datetime
    parameter_normalized: str
    value: float
    unit: str
    aqi_category: str
    aqi_score: float
    health_risk_level: int
    pollution_spike: bool

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class SocialPostData(BaseModel):
    post_id: str
    event_time_utc: datetime
    title_normalized: str
    body_normalized: str
    author_normalized: str
    source: str
    upvotes: int
    keywords: List[str]
    regions_detected: List[str]

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class SocialSentimentData(BaseModel):
    post_id: str
    event_time_utc: datetime
    sentiment_label: str
    sentiment_category: str
    sentiment_confidence_pct: float
    engagement_score: float

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class DailyAggregateData(BaseModel):
    date: str
    city: str
    avg_temperature: Optional[float]
    avg_feels_like: Optional[float]
    max_temperature: Optional[float]
    min_temperature: Optional[float]
    avg_humidity: Optional[float]
    total_precipitation: Optional[float]
    avg_wind_speed: Optional[float]
    avg_aqi_score: Optional[float]
    avg_health_risk: Optional[float]
    num_social_posts: int
    avg_sentiment_confidence: Optional[float]
    positive_posts: int
    negative_posts: int
    neutral_posts: int
    anomaly_count: int
    pollution_spike_count: int

class HealthStatus(BaseModel):
    status: str
    cassandra: str
    tables: Dict[str, int]
    timestamp: datetime

class SentimentSummary(BaseModel):
    total_posts: int
    positive: int
    negative: int
    neutral: int
    positive_pct: float
    negative_pct: float
    neutral_pct: float
    avg_confidence: float

class CorrelationMetrics(BaseModel):
    sentiment_weather_strong: int
    sentiment_weather_moderate: int
    sentiment_weather_weak: int
    sentiment_pollution_strong: int
    sentiment_pollution_moderate: int
    sentiment_pollution_weak: int

# ============================================================
# Cassandra Connection Manager
# ============================================================

class CassandraConnection:
    def __init__(self):
        self.cluster = None
        self.session = None

    def connect(self):
        """Connect to Cassandra cluster"""
        try:
            auth_provider = PlainTextAuthProvider(
                username=CASSANDRA_USER,
                password=CASSANDRA_PASSWORD
            )
            self.cluster = Cluster(
                [CASSANDRA_HOST],
                port=CASSANDRA_PORT,
                auth_provider=auth_provider
            )
            self.session = self.cluster.connect(CASSANDRA_KEYSPACE)
            logger.info(f"✓ Connected to Cassandra: {CASSANDRA_HOST}:{CASSANDRA_PORT}")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to connect to Cassandra: {str(e)}")
            return False

    def disconnect(self):
        """Disconnect from Cassandra"""
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()
        logger.info("✓ Disconnected from Cassandra")

    def execute(self, query: str, params: List = None):
        """Execute CQL query"""
        if not self.session:
            raise Exception("Not connected to Cassandra")
        if params:
            return self.session.execute(query, params)
        return self.session.execute(query)

# ============================================================
# FastAPI Application Setup
# ============================================================

db = CassandraConnection()

async def lifespan(app: FastAPI):
    """Manage application lifespan"""
    logger.info("=" * 60)
    logger.info("CLIMATE & SENTIMENT TRACKER API - STARTUP")
    logger.info("=" * 60)
    if not db.connect():
        logger.error("Failed to initialize Cassandra connection")
        raise Exception("Cannot connect to Cassandra")
    logger.info("✓ API startup complete")
    yield
    logger.info("API shutting down...")
    db.disconnect()

app = FastAPI(
    title="Climate & Sentiment Tracker API",
    description="REST API for Boston climate and sentiment data from Cassandra",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# Health Check Endpoint
# ============================================================

@app.get("/health", response_model=HealthStatus)
async def health_check():
    """Health check endpoint with table statistics"""
    try:
        tables = {}
        for table in ["weather_data", "air_quality_data", "social_posts", "social_sentiment", "daily_aggregates"]:
            try:
                result = db.execute(f"SELECT COUNT(*) as count FROM {table}")
                row = result.one()
                tables[table] = row.count if row else 0
            except Exception as e:
                logger.warning(f"Could not count {table}: {str(e)}")
                tables[table] = 0
        return HealthStatus(
            status="healthy",
            cassandra="connected",
            tables=tables,
            timestamp=datetime.utcnow()
        )
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

# ============================================================
# Weather Endpoints
# ============================================================

@app.get("/api/v1/weather/latest", response_model=List[WeatherData])
async def get_latest_weather(
    limit: int = Query(10, ge=1, le=100),
    anomaly_only: bool = Query(False)
):
    """Get latest weather data"""
    try:
        query = "SELECT * FROM weather_data WHERE city='Boston, USA' ORDER BY event_time_utc DESC LIMIT %s"
        result = db.execute(query, [limit])
        rows = result.all()
        data = []
        for row in rows:
            if anomaly_only and row.weather_anomaly == "Normal":
                continue
            data.append(WeatherData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                temperature_celsius=row.temperature_celsius,
                feels_like_temp=row.feels_like_temp,
                humidity=row.humidity,
                precipitation=row.precipitation,
                wind_speed=row.wind_speed,
                wind_category=row.wind_category,
                weather_anomaly=row.weather_anomaly
            ))
        return data
    except Exception as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/weather/anomalies", response_model=List[WeatherData])
async def get_weather_anomalies(limit: int = Query(50, ge=1, le=500)):
    """Get anomalous weather events"""
    try:
        query = """
        SELECT * FROM weather_data 
        WHERE city='Boston, USA' AND weather_anomaly != 'Normal'
        ORDER BY event_time_utc DESC
        LIMIT %s
        """
        result = db.execute(query, [limit])
        rows = result.all()
        return [
            WeatherData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                temperature_celsius=row.temperature_celsius,
                feels_like_temp=row.feels_like_temp,
                humidity=row.humidity,
                precipitation=row.precipitation,
                wind_speed=row.wind_speed,
                wind_category=row.wind_category,
                weather_anomaly=row.weather_anomaly
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching weather anomalies: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Air Quality Endpoints
# ============================================================

@app.get("/api/v1/air-quality/latest", response_model=List[AirQualityData])
async def get_latest_air_quality(
    limit: int = Query(10, ge=1, le=100),
    parameter: Optional[str] = Query(None),
    spike_only: bool = Query(False)
):
    """Get latest air quality data"""
    try:
        if parameter:
            query = """
            SELECT * FROM air_quality_data 
            WHERE city='Boston, USA' AND parameter_normalized=%s
            ORDER BY event_time_utc DESC
            LIMIT %s
            """
            result = db.execute(query, [parameter, limit])
        else:
            query = """
            SELECT * FROM air_quality_data 
            WHERE city='Boston, USA'
            ORDER BY event_time_utc DESC
            LIMIT %s
            """
            result = db.execute(query, [limit])
        rows = result.all()
        data = []
        for row in rows:
            if spike_only and not row.pollution_spike:
                continue
            data.append(AirQualityData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                parameter_normalized=row.parameter_normalized,
                value=row.value,
                unit=row.unit,
                aqi_category=row.aqi_category,
                aqi_score=row.aqi_score,
                health_risk_level=row.health_risk_level,
                pollution_spike=row.pollution_spike
            ))
        return data
    except Exception as e:
        logger.error(f"Error fetching air quality data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/air-quality/pollution-spikes", response_model=List[AirQualityData])
async def get_pollution_spikes(limit: int = Query(50, ge=1, le=500)):
    """Get pollution spike events"""
    try:
        query = """
        SELECT * FROM air_quality_data 
        WHERE city='Boston, USA' AND pollution_spike=true
        ORDER BY event_time_utc DESC
        LIMIT %s
        """
        result = db.execute(query, [limit])
        rows = result.all()
        return [
            AirQualityData(
                city=row.city,
                event_time_utc=row.event_time_utc,
                parameter_normalized=row.parameter_normalized,
                value=row.value,
                unit=row.unit,
                aqi_category=row.aqi_category,
                aqi_score=row.aqi_score,
                health_risk_level=row.health_risk_level,
                pollution_spike=row.pollution_spike
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching pollution spikes: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Social Posts Endpoints
# ============================================================

@app.get("/api/v1/social/posts/latest", response_model=List[SocialPostData])
async def get_latest_posts(
    limit: int = Query(20, ge=1, le=100),
    source: Optional[str] = Query(None)
):
    """Get latest social posts"""
    try:
        query = """
        SELECT * FROM social_posts 
        ORDER BY event_time_utc DESC
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit])
        rows = result.all()
        data = []
        for row in rows:
            if source and row.source != source:
                continue
            data.append(SocialPostData(
                post_id=row.post_id,
                event_time_utc=row.event_time_utc,
                title_normalized=row.title_normalized,
                body_normalized=row.body_normalized,
                author_normalized=row.author_normalized,
                source=row.source,
                upvotes=row.upvotes,
                keywords=list(row.keywords) if row.keywords else [],
                regions_detected=list(row.regions_detected) if row.regions_detected else []
            ))
        return data
    except Exception as e:
        logger.error(f"Error fetching social posts: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Sentiment Analysis Endpoints
# ============================================================

@app.get("/api/v1/sentiment/latest", response_model=List[SocialSentimentData])
async def get_latest_sentiment(
    limit: int = Query(20, ge=1, le=100),
    label: Optional[str] = Query(None)
):
    """Get latest sentiment predictions"""
    try:
        if label and label.lower() in ["positive", "negative", "neutral"]:
            query = """
            SELECT * FROM social_sentiment 
            WHERE sentiment_label=%s
            ORDER BY event_time_utc DESC
            LIMIT %s
            """
            result = db.execute(query, [label.lower(), limit])
        else:
            query = """
            SELECT * FROM social_sentiment 
            ORDER BY event_time_utc DESC
            LIMIT %s
            ALLOW FILTERING
            """
            result = db.execute(query, [limit])
        rows = result.all()
        return [
            SocialSentimentData(
                post_id=row.post_id,
                event_time_utc=row.event_time_utc,
                sentiment_label=row.sentiment_label,
                sentiment_category=row.sentiment_category,
                sentiment_confidence_pct=row.sentiment_confidence_pct,
                engagement_score=row.engagement_score
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching latest sentiment: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/sentiment/summary", response_model=SentimentSummary)
async def get_sentiment_summary(
    days: int = Query(1, ge=1, le=90)
):
    """Get sentiment summary for the last N days"""
    try:
        cutoff = datetime.utcnow() - timedelta(days=days)
        query = """
        SELECT * FROM social_sentiment
        WHERE event_time_utc >= %s
        ALLOW FILTERING
        """
        rows = db.execute(query, [cutoff]).all()
        positive = negative = neutral = 0
        total_confidence = 0.0
        count = 0
        for row in rows:
            label = (row.sentiment_label or "neutral").lower()
            if label == "positive":
                positive += 1
            elif label == "negative":
                negative += 1
            else:
                neutral += 1
            total_confidence += row.sentiment_confidence_pct or 0.0
            count += 1
        total = positive + negative + neutral
        return SentimentSummary(
            total_posts=total,
            positive=positive,
            negative=negative,
            neutral=neutral,
            positive_pct=round((positive / total * 100) if total else 0, 2),
            negative_pct=round((negative / total * 100) if total else 0, 2),
            neutral_pct=round((neutral / total * 100) if total else 0, 2),
            avg_confidence=round(total_confidence / count if count else 0, 2)
        )
    except Exception as e:
        logger.error(f"Error calculating sentiment summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Analytics Endpoints
# ============================================================

@app.get("/api/v1/analytics/daily", response_model=List[DailyAggregateData])
async def get_daily_aggregates(
    days: int = Query(30, ge=1, le=90)
):
    """Get daily aggregated metrics"""
    try:
        query = """
        SELECT * FROM daily_aggregates 
        WHERE city='Boston, USA'
        ORDER BY date DESC
        LIMIT %s
        """
        result = db.execute(query, [days])
        rows = result.all()
        return [
            DailyAggregateData(
                date=str(row.date),
                city=row.city,
                avg_temperature=row.avg_temperature,
                avg_feels_like=row.avg_feels_like,
                max_temperature=row.max_temperature,
                min_temperature=row.min_temperature,
                avg_humidity=row.avg_humidity,
                total_precipitation=row.total_precipitation,
                avg_wind_speed=row.avg_wind_speed,
                avg_aqi_score=row.avg_aqi_score,
                avg_health_risk=row.avg_health_risk,
                num_social_posts=row.num_social_posts,
                avg_sentiment_confidence=row.avg_sentiment_confidence,
                positive_posts=row.positive_posts,
                negative_posts=row.negative_posts,
                neutral_posts=row.neutral_posts,
                anomaly_count=row.anomaly_count,
                pollution_spike_count=row.pollution_spike_count
            )
            for row in rows
        ]
    except Exception as e:
        logger.error(f"Error fetching daily aggregates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/analytics/correlation", response_model=CorrelationMetrics)
async def get_correlation_metrics(days: int = Query(30, ge=1, le=90)):
    """Get sentiment-environmental correlation metrics"""
    try:
        query = """
        SELECT * FROM daily_aggregates 
        WHERE city='Boston, USA'
        ORDER BY date DESC
        LIMIT %s
        """
        result = db.execute(query, [days])
        rows = result.all()
        strong_weather = sum(getattr(r, 'strong_sentiment_weather_correlation', 0) for r in rows)
        strong_pollution = sum(getattr(r, 'strong_sentiment_pollution_correlation', 0) for r in rows)
        return CorrelationMetrics(
            sentiment_weather_strong=strong_weather,
            sentiment_weather_moderate=0,
            sentiment_weather_weak=0,
            sentiment_pollution_strong=strong_pollution,
            sentiment_pollution_moderate=0,
            sentiment_pollution_weak=0
        )
    except Exception as e:
        logger.error(f"Error fetching correlation metrics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Search & Filter Endpoints
# ============================================================

@app.get("/api/v1/search/keywords")
async def search_by_keywords(
    keywords: str = Query(..., description="Comma-separated keywords"),
    limit: int = Query(20, ge=1, le=100)
):
    """Search social posts by keywords"""
    try:
        keyword_list = [k.strip().lower() for k in keywords.split(",")]
        query = """
        SELECT * FROM social_posts 
        ORDER BY event_time_utc DESC
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit * 3])
        rows = result.all()
        filtered = []
        for row in rows:
            row_keywords = [k.lower() for k in (row.keywords or [])]
            if any(kw in row_keywords for kw in keyword_list):
                filtered.append(SocialPostData(
                    post_id=row.post_id,
                    event_time_utc=row.event_time_utc,
                    title_normalized=row.title_normalized,
                    body_normalized=row.body_normalized,
                    author_normalized=row.author_normalized,
                    source=row.source,
                    upvotes=row.upvotes,
                    keywords=list(row.keywords) if row.keywords else [],
                    regions_detected=list(row.regions_detected) if row.regions_detected else []
                ))
                if len(filtered) >= limit:
                    break
        return filtered
    except Exception as e:
        logger.error(f"Error searching keywords: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/search/regions")
async def search_by_region(
    region: str = Query(..., description="Boston neighborhood"),
    limit: int = Query(20, ge=1, le=100)
):
    """Search posts by Boston region/neighborhood"""
    try:
        query = """
        SELECT * FROM social_posts 
        ORDER BY event_time_utc DESC
        LIMIT %s
        ALLOW FILTERING
        """
        result = db.execute(query, [limit * 3])
        rows = result.all()
        filtered = []
        for row in rows:
            regions = [r.lower() for r in (row.regions_detected or [])]
            if region.lower() in regions:
                filtered.append(SocialPostData(
                    post_id=row.post_id,
                    event_time_utc=row.event_time_utc,
                    title_normalized=row.title_normalized,
                    body_normalized=row.body_normalized,
                    author_normalized=row.author_normalized,
                    source=row.source,
                    upvotes=row.upvotes,
                    keywords=list(row.keywords) if row.keywords else [],
                    regions_detected=list(row.regions_detected) if row.regions_detected else []
                ))
                if len(filtered) >= limit:
                    break
        return filtered
    except Exception as e:
        logger.error(f"Error searching regions: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# ============================================================
# Root Endpoint
# ============================================================

@app.get("/")
async def root():
    """Root endpoint with API documentation"""
    return {
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

# ============================================================
# Main Entry Point
# ============================================================

if __name__ == "__main__":
    logger.info("Starting Climate & Sentiment Tracker API...")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        log_level="info"
    )

