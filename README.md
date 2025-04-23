# 🚕 Real-Time Ride-Hailing Stream Analytics

## Overview

This project simulates a real-time analytics pipeline for ride-hailing services like Uber, Cabify, and Lyft. It processes massive streaming data — ride requests, driver statuses, trip updates — to extract meaningful business insights.

## 🌟 Objectives

### General Objective:
Develop a real-time analytics pipeline that processes ride-hailing events and generates insights using modern stream processing technologies.

### Specific Objectives:
- Simulate ride-hailing events (ride requests, driver updates).
- Stream events using Azure EventHub.
- Process data using Spark Streaming.
- Store structured data (Parquet in Azure Blob Storage).
- Perform real-time analytics to derive actionable insights.
- Visualize key metrics in a dashboard.

## 📊 Features

- **Data Generator**: Simulates realistic ride-hailing scenarios (Madrid-based).
- **Streaming Pipeline**: Azure EventHub + Spark Streaming (Colab).
- **Analytics**: Demand-supply, ride durations, anomaly detection.
- **Dashboard**: Real-time visual insights (Streamlit/PowerBI - optional).

## 💠 Tech Stack

- Python
- Azure EventHub
- Spark Streaming (Google Colab)
- fastavro, Faker, geopy
- Azure Blob Storage (Parquet)
- Optional: Streamlit / PowerBI for visualization

## 📂 Project Structure

- `Ride hailing Simulation.py`: Python script generating synthetic ride and driver events.
- `Group_8_connection_to_Blob.ipynb`: Notebook for connecting and storing data in Azure Blob Storage.
- Avro schemas for structured, efficient data serialization.
- Kafka-compatible Azure EventHub for real-time event streaming.

## 🚀 Setup Instructions

1. **Clone the repo**:
   ```bash
   git clone https://github.com/yourusername/ride-hailing-analytics.git
   cd ride-hailing-analytics
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run data generator**:
   ```bash
   python "Ride hailing Simulation.py"
   ```

4. **Run Spark Streaming pipeline**:
   - Use `Group_8_connection_to_Blob.ipynb` in Google Colab to process and store events.

## 📈 Analytics Use Cases

- **Basic Use Case**:
  - Hourly Ride Metrics: total rides, average ETA to pickup, average fare, average traffic multiplier (tumbling window by hour).

- **Intermediate Use Case**:
  - Demand-Supply Mismatch: compare rides requested vs. active drivers per hour.
  - Passenger Satisfaction: average driver rating by hour.

- **Advanced Use Case**:
  - Surge Pricing & Anomaly Detection: flag unusually high-fare short trips under light traffic, map average surge multiplier by geo-grid.
  - Driver Utilization & Workload: total rides served per driver, average ride distance, top 10 busiest drivers.
  - Coverage Heatmap: 2D density map of driver locations across Madrid to spot hot/cold zones.

## 📝 Reflection Summary

- Learned to simulate complex data patterns.
- Built scalable real-time pipelines.
- Applied stream analytics to practical, business-relevant cases.

---
