# 🇮🇱 Israel Pension Fund Tracker

A lightweight, open-source visualization dashboard for Israeli Pension and Gemel fund holdings. This project provides a transparent, hierarchical view of how public funds are invested, down to the specific asset level.

**[View Live Demo](https://mat-mo.github.io/Israel-Pension-Tracker)**

## 📊 Features

* **Pretty Visualization:** Interactive deep-dive into asset allocation (Asset Class → Sub-class → Specific Holding).
* **Detailed Breakdown:** View holdings for Stocks, Bonds, Real Estate, Alternative Investments, and more.
* **Track Switching:** Compare different investment tracks (e.g., General Track vs. S&P 500).
* **Zero-Dependency:** Built with pure HTML/JS and Tailwind CSS (via CDN). No build process required.
* **Mobile Responsive:** Works seamlessly on desktop and mobile devices.

## 🛠️ Tech Stack

* **Frontend:** HTML5, Vanilla JavaScript
* **Styling:** [Tailwind CSS](https://tailwindcss.com/)
* **Visualization:** [Apache ECharts](https://echarts.apache.org/)
* **Data:** Processed JSON generated from regulatory Excel/CSV reports.

## 📂 Project Structure

```text
├── index.html        # The main dashboard application
├── data.json         # The processed holdings data (The "Database")
├── process_data.py   # (Optional) Script used to convert raw CSVs to JSON
└── README.md         # Documentation
