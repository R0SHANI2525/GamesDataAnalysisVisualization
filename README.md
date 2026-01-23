# Gamers Voice 🎮  
### Steam Game Analytics & Review Insights

Gamers Voice is a data analytics project focused on analyzing Steam game metadata and user reviews to understand player sentiment, game performance, and market trends. The project follows a cloud-first data engineering approach and uses Python-based analysis for insights generation.

---

## 📌 Problem Statement

Player engagement and review behavior on Steam are influenced by multiple factors such as game genre, pricing, platform support, and developer reputation. However, the relationship between these factors and user sentiment is not always clear. This project aims to analyze Steam data to identify patterns that drive positive and negative player feedback.

---

## 🎯 Objectives

- Analyze large-scale Steam game and review data  
- Understand user sentiment through review analysis  
- Perform exploratory data analysis on game metadata  
- Clean and merge multiple datasets into a master dataset  
- Prepare analytics-ready data for dashboard visualization  

---

## 📂 Dataset Source

- **Source:** Kaggle  
- **Dataset:** Steam Dataset 2025 – Multi-Modal Gaming Analytics  
- **Link:** https://www.kaggle.com/datasets/crainbramp/steam-dataset-2025-multi-modal-gaming-analytics  

The dataset contains Steam game metadata, normalized relational tables, and over 1 million user reviews.

---

## ☁️ Data Ingestion Workflow

- Dataset accessed from Kaggle  
- An **AWS EC2 instance** was used as the compute environment  
- Data uploaded **directly to Amazon S3** (no local download)  
- Secure access established using **MobaXterm (SSH)**  

This approach simulates a real-world cloud data ingestion pipeline.

---

## 🧪 Data Analysis & Processing

- Exploratory Data Analysis (EDA) performed separately on:
  - Steam applications metadata
  - Steam user reviews
- Data quality checks, missing value analysis, and distributions explored
- Multiple normalized tables merged using bridge tables
- A **consolidated master dataset** created for downstream analytics

All EDA and processing steps are documented in Jupyter notebooks.

---

## 🗂️ Repository Structure

```text
Gamers_Voice/
│
├── data_source/        # Dataset reference & EC2-based ingestion scripts
├── notebooks/          # EDA and data processing notebooks
│   ├── eda/
│   └── processing/
├── README.md
