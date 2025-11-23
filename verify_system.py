#!/usr/bin/env python3

import requests
import time
import json
import sys

BASE_URL = "http://localhost:8000"

def log(message, status="INFO"):
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {status}: {message}")

def check_health():
    try:
        response = requests.get(f"{BASE_URL}/health")
        if response.status_code == 200:
            data = response.json()
            log(f"Health Check Passed: {data['status']}")
            log(f"Table Counts: {json.dumps(data['tables'], indent=2)}")
            return True
        else:
            log(f"Health Check Failed: {response.status_code} - {response.text}", "ERROR")
            return False
    except Exception as e:
        log(f"Health Check Exception: {e}", "ERROR")
        return False

def test_endpoint(endpoint, name):
    try:
        url = f"{BASE_URL}{endpoint}"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                count = len(data)
                log(f"{name}: Retrieved {count} items")
            else:
                log(f"{name}: {json.dumps(data, indent=2)}")
            return True
        else:
            log(f"{name} Failed: {response.status_code}", "ERROR")
            return False
    except Exception as e:
        log(f"{name} Exception: {e}", "ERROR")
        return False

def main():
    log("Starting system verification...")
    log("=" * 60)
    
    if not check_health():
        log("Health check failed. Retrying in 5 seconds...", "WARN")
        time.sleep(5)
        if not check_health():
            log("Health check failed after retry. Exiting.", "ERROR")
            sys.exit(1)
    
    endpoints = [
        ("/api/v1/weather/latest?limit=5", "Weather Data"),
        ("/api/v1/air-quality/latest?limit=5", "Air Quality Data"),
        ("/api/v1/social/posts/latest?limit=5", "Social Posts"),
        ("/api/v1/social/sentiment/summary", "Sentiment Summary"),
    ]
    
    results = []
    for endpoint, name in endpoints:
        result = test_endpoint(endpoint, name)
        results.append((name, result))
    
    log("=" * 60)
    log("Verification Summary:")
    for name, result in results:
        status = "PASS" if result else "FAIL"
        log(f"  {name}: {status}")
    
    all_pass = all(result for _, result in results)
    if all_pass:
        log("All tests passed!", "INFO")
        sys.exit(0)
    else:
        log("Some tests failed", "ERROR")
        sys.exit(1)

if __name__ == "__main__":
    main()
ifisinstance(data,list):
                count=len(data)
log(f"{name}: Success (Returned {count} items)")
ifcount>0:


                    pass
else:
                log(f"{name}: Success (Returned object)")

returnTrue
else:
            log(f"{name}: Failed {response.status_code} - {response.text}","ERROR")
returnFalse
exceptExceptionase:
        log(f"{name}: Exception {e}","ERROR")
returnFalse

defmain():
    log("Starting System Verification...")


ifnotcheck_health():
        log("Aborting verification due to health check failure.","CRITICAL")
return


test_endpoint("/api/v1/weather/latest?limit=5","Latest Weather")
test_endpoint("/api/v1/weather/anomalies?limit=5","Weather Anomalies")


test_endpoint("/api/v1/air-quality/latest?limit=5","Latest Air Quality")
test_endpoint("/api/v1/air-quality/pollution-spikes?limit=5","Pollution Spikes")


test_endpoint("/api/v1/social/posts/latest?limit=5","Latest Social Posts")
test_endpoint("/api/v1/search/keywords?keywords=climate,heat&limit=5","Keyword Search")
test_endpoint("/api/v1/search/regions?region=Downtown&limit=5","Region Search")


test_endpoint("/api/v1/sentiment/latest?limit=5","Latest Sentiment")
test_endpoint("/api/v1/sentiment/summary?days=30","Sentiment Summary")


test_endpoint("/api/v1/analytics/daily?days=7","Daily Analytics")
test_endpoint("/api/v1/analytics/correlation?days=30","Correlation Metrics")

log("Verification Complete.")

if__name__=="__main__":
    main()
