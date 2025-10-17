# ARGO Float Explorer 🌊

[![Python](https://img.shields.io/badge/Python-3.8%252B-blue)](https://www.python.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28%252B-red)](https://streamlit.io/)
[![RAG](https://img.shields.io/badge/RAG-AI--powered-orange)](#)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)

## Overview
ARGO Float Explorer is a comprehensive oceanographic data analysis and visualization platform for ARGO profiling floats. The system integrates an **AI-powered chatbot interface** with interactive visualizations to provide insights into ocean parameters like temperature, salinity, and biogeochemical measurements.

## ✨ Key Features

### 🌍 Interactive Dashboard
- Streamlit-based interface for real-time exploration of ARGO floats.

### 📊 Advanced Visualizations
- Temperature/salinity profiles, time series, T-S diagrams, and trajectory mapping.
- Dynamic graph generation with intelligent visualization selection.

### 🤖 AI-Powered Chatbot (FloatChat)
- Natural language queries converted into RAG-enhanced SQL queries.
- Requires **Perplexity API key** (set in `config.py`) for semantic search and AI-assisted query generation.
- Supports multi-table joins across 15+ normalized tables.
- Conversational interface for intuitive data exploration.

### 🔧 Data Processing
- Automated parsing and processing of ARGO NetCDF files (`*.nc`) directly in the project root.
- Metadata, profile, and trajectory parsing pipelines.
- Built-in quality control (QC) flag handling and validation.

### 🗄️ Database Management
- PostgreSQL backend with optimized schema.
- Vector database (`argo_vector_db.index`) for AI semantic search.

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL
- **Perplexity API key** (set in `config.py`)
- Required Python packages:  
`streamlit`, `pandas`, `numpy`, `xarray`, `netCDF4`, `plotly`, `psycopg2`, `folium`  
(install via `pip install <package>`)

### Installation & Setup
```bash
# Clone the repository:
git clone https://github.com/NeilLandge/FloatChat-AI---Argo-Float-Explorer.git
cd FloatChat-AI---Argo-Float-Explorer
```

---

Update your PostgreSQL credentials and Perplexity API key in ⚙️ Configuration/config.py.

---

Launch the Streamlit dashboard:
```bash
streamlit run 🎨 Visualization\ &\ UI/dashboard.py
```

---

# 📁 Project Structure

```
FloatChat ARGO/
├── 📊 Data Processing
│   ├── parser.py
│   ├── process.py
│   ├── temporary_meta_parser.py
│   ├── temporary_profile_parser.py
│   └── temporary_trajectory_parser.py
│
├── 🤖 AI & Database
│   ├── setup_rag.py
│   ├── test_rag.py
│   ├── db_connection.py
│   ├── argo_vector_db.index
│   └── argo_documents.pkl
│
├── 🎨 Visualization & UI
│   ├── dashboard.py
│   ├── graph_generator.py
│   └── script.py
│
├── ⚙️ Configuration
│   ├── config.py
│   └── database
│
├── *.nc                             # ARGO NetCDF files (profiles, metadata, trajectories)
├── argo_metadata
├── argo_float_schema_connected
├── screenshots                      # Dashboard and graph screenshots
│   ├── dashboard.png
│   ├── enhancedragassistant.png
│   ├── floatlocations.png
│   ├── surfacetemptrends.png
│   └── tempandsalinityprofiles.png
├── README.md
└── .gitignore
```

### 📸 Screenshots

Here are some sample screenshots of the FloatChat ARGO dashboard and visualizations:

![Dashboard](https://raw.githubusercontent.com/NeilLandge/FloatChat-AI---Argo-Float-Explorer/main/screenshots/dashboard.png)
![RAG Assistant](https://raw.githubusercontent.com/NeilLandge/FloatChat-AI---Argo-Float-Explorer/main/screenshots/enhancedragassistant.png)
![Float Locations](https://raw.githubusercontent.com/NeilLandge/FloatChat-AI---Argo-Float-Explorer/main/screenshots/floatlocations.png)

---

### 🔧 Workflow

Data Ingestion: Parse .nc files and store in PostgreSQL.

Semantic Indexing: Create vector database for AI-assisted queries (Perplexity API used here).

Query Processing: Convert natural language → RAG context → SQL queries.

Visualization: Auto-generate professional plots and trajectory maps.

Interactive Delivery: Display results in chatbot and Streamlit dashboard.

### 🎯 Highlights

Conversational interface for oceanographic data exploration.

Dynamic graph generation and intelligent visualization selection.

Fully integrated pipeline: parsing → database → AI → visualization.

Requires Perplexity API key for AI-powered insights.

### 🤝 Authors

Srinidhi Kulkarni

Neil Landge

Mahi Kulkarni

Mohit Dargude

Shreya Ghuse

Rohan Salunkhe

### 📄 License

This project is licensed under the MIT License.

### 🌊 Repository Link

You can clone the project here:
https://github.com/NeilLandge/FloatChat-AI---Argo-Float-Explorer

### 🌊 Explore the oceans with AI

FloatChat makes ARGO oceanographic data accessible through conversation and visualization.
