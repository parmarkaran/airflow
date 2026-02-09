"# airflow" 
uv venv venv
venv\Scripts\activate

# 🚀 MrBeast YouTube Analytics Pipeline

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017EFE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![YouTube API](https://img.shields.io/badge/YouTube%20Data%20API-FF0000?style=for-the-badge&logo=youtube&logoColor=white)

## 📖 Project Overview
This project is an end-to-end Data Engineering pipeline designed to extract, transform, and analyze data from the **MrBeast YouTube Channel**.

Using **Apache Airflow** for orchestration, this pipeline automates the retrieval of video statistics (views, likes, comments) and channel metrics to identify trends in viral growth.

## 🏗️ Architecture
The pipeline follows a standard ETL (Extract, Transform, Load) workflow:

1.  **Extract:** Python scripts interact with the **YouTube Data API v3** to fetch raw JSON data for videos and channel stats.
2.  **Transform:** Data is cleaned, normalized, and structured using **Pandas**.
3.  **Load:** Processed data is stored in **PostgreSQL** for analysis.
4.  **Orchestrate:** **Apache Airflow** schedules and monitors the entire workflow daily.

## 🛠️ Tech Stack
* **Orchestration:** Apache Airflow (running in Docker)
* **Language:** Python 3.9+
* **Libraries:** `pandas`, `google-api-python-client`, `psycopg2`
* **Containerization:** Docker & Docker Compose
* **Source:** YouTube Data API v3

## 📂 Repository Structure
```bash
├── dags/
│   ├── mrbeast_etl_dag.py    # Main Airflow DAG definition
│   └── scripts/              # Python ETL helper scripts
├── logs/                     # Airflow logs
├── docker-compose.yaml       # Docker setup
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation

git clone [https://github.com/parmarkaran/airflow.git](https://github.com/parmarkaran/airflow.git)
cd airflow

Step 2: API Configuration
Go to the Google Cloud Console.

Create a project and enable the YouTube Data API v3.

Generate an API Key.

Create a file named .env in the root directory and add your key:

Bash
YOUTUBE_API_KEY=your_api_key_here
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
Step 3: Run with Docker
Initialize the Airflow database and start the services:

Bash
docker-compose up airflow-init
docker-compose up -d
Step 4: Access the UI
Open your browser and navigate to http://localhost:8081.

Username: airflow

Password: airflow

Trigger the mrbeast_analytics_dag to see the pipeline in action!

📊 Data Schema
The pipeline extracts the following metrics for every video:

video_id: Unique YouTube ID

title: Video Title

published_at: Date of upload

view_count: Total views

like_count: Total likes

comment_count: Total comments

🚀 Future Improvements
Add Sentiment Analysis on video comments using NLTK.

Visualize the data using PowerBI or Metabase.

Deploy the Airflow instance to AWS MWAA.

🤝 Contributing
Contributions, issues, and feature requests are welcome! Feel free to check the issues page.

📝 License
This project is licensed under the MIT License.