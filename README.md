# Real-Time Operational Analytics Pipeline (Yelp Data)

## 📌 Project Overview
This project implements an end-to-end Big Data pipeline designed to reduce "Information Latency" in the hospitality industry. It processes large-scale unstructured JSON data and simulates a real-time streaming architecture to detect critical reputation risks (e.g., 1-star reviews) instantaneously.

## 🏗 Architecture
The solution is built on a hybrid architecture:
1.  **Batch Layer (ETL):** A memory-optimized Python pipeline processing raw Yelp logs (4GB+ JSON) into structured CSVs.
2.  **Speed Layer (Streaming):** A Pub/Sub streaming prototype (simulating Apache Kafka) for real-time risk detection.
3.  **Analytics Layer (Viz):** Interactive Tableau dashboards for geospatial and sentiment analysis.

## 🛠 Tech Stack
* **Language:** Python 3.x (ETL, Simulation Logic)
* **Streaming Pattern:** Publish/Subscribe (Kafka Prototype)
* **Visualization:** Tableau Desktop (Geospatial & Statistical Analysis)
* **Data:** Yelp Open Dataset (JSON/Unstructured)

## 🚀 How to Run
1.  **Clone the repo:**
    ```bash
    git clone [https://github.com/yourusername/yelp-realtime-pipeline.git](https://github.com/yourusername/yelp-realtime-pipeline.git)
    ```
2.  **Run the Streaming Prototype:**
    ```bash
    python streaming_pipeline_prototype.py
    ```
    *Note: This module runs in "Simulation Mode" and does not require a local Zookeeper instance.*

## 📊 Visualizations
### Geospatial Performance Map
![Map Analysis](dashboard_screenshots/dashboard_screenshot_1.png)
*Visualizing underperforming clusters in metropolitan areas.*

### Real-Time Alert Logic
![Streaming Logs](dashboard_screenshots/terminal_output.png)
*Terminal output showing real-time 1-star review filtering.*
