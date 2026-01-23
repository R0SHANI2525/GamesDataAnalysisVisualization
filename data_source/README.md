# Data Source and Ingestion

## Dataset Source

The dataset used in this project is the **Steam Dataset 2025 – Multi-Modal Gaming Analytics**, sourced from Kaggle.

🔗 Dataset Link:  
https://www.kaggle.com/datasets/crainbramp/steam-dataset-2025-multi-modal-gaming-analytics

The dataset contains large-scale Steam game metadata, normalized relational tables, and over one million user reviews, making it suitable for large-scale analytics and sentiment analysis.

---

## Cloud-Based Data Ingestion

The dataset was ingested directly into the cloud without downloading it to a local machine. An **AWS EC2 instance** was used as the compute environment to access the Kaggle dataset and upload the data directly into an **Amazon S3 bucket**.

A secure SSH connection was established using **MobaXterm**, enabling controlled and scalable data transfer. This cloud-first approach avoids local storage limitations and follows real-world data engineering practices.

---

## Files in This Directory

- `uploader_script.py` – Script used on the EC2 instance to upload dataset files to Amazon S3
- `unzipper.py` – Script used to extract compressed dataset files before upload
- `mobaxterm_proof.rtf` – Session log demonstrating secure SSH-based ingestion (optional)
