# Copying Hudi Metadata Across Environments

## Overview

This process copies **Hudi metadata** (without actual data) from **NADTA** to **EUDTA, APDTA, and LADTA** across all environments: **DEV, TST, and PRD**.

### Why?
- Preserves **Hudi table structure** while keeping it empty.
- Ensures **Athena & Spark recognize the table** without old transaction history.
- **Excludes unnecessary files**:
  - **Parquet files** (actual data)
  - **Log files**
  - **Commit, clean, and inflight metadata** (to avoid unwanted history)
  - **Archived metadata files**

---

## Tables Being Processed

The following tables are copied across all environments:

### **EUDTA Tables**
- `F594217H`
- `F594947H`
- `F594941H`
- `F594215H`

### **APDTA Tables**
- `F594217H`
- `F594947H`
- `F594941H`
- `F594215H`

### **LADTA Tables**
- `F594217H`
- `F594947H`
- `F594941H`
- `F594215H`

Each of these tables is copied from **NADTA** in the respective environment.

---

## Commands to Run

### 📌 **Copy Hudi Metadata for All Environments**
Run the following AWS CLI commands to **copy metadata only** (excluding Parquet and unnecessary logs) in **DEV, TST, and PRD**.

### **DEV Environment**
```bash
aws s3 cp s3://baxaws-dev-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594217H/ s3://baxaws-dev-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594217H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-dev-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594947H/ s3://baxaws-dev-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594947H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-dev-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594941H/ s3://baxaws-dev-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594941H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml
```

### **Test Environment**
```bash
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594217H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594217H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594947H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594947H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594941H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594941H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594215H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594215H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594217H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/APDTA/F594217H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594947H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/APDTA/F594947H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594941H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/APDTA/F594941H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594215H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/APDTA/F594215H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594217H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/LADTA/F594217H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594947H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/LADTA/F594947H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594941H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/LADTA/F594941H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594215H/ s3://baxaws-tst-enterpriseanalytics-edh-jde-inbound/raw-consolidated/LADTA/F594215H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml
```

### **PROD Environment**
```bash
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594217H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594217H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594947H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594947H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594941H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594941H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594215H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/EUDTA/F594215H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594217H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/APDTA/F594217H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594947H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/APDTA/F594947H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594941H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/APDTA/F594941H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594215H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/APDTA/F594215H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594217H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/LADTA/F594217H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594947H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/LADTA/F594947H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594941H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/LADTA/F594941H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml && \
aws s3 cp s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/NADTA/F594215H/ s3://baxaws-prd-enterpriseanalytics-edh-jde-inbound/raw-consolidated/LADTA/F594215H/ --recursive --exclude "*.parquet" --exclude "*.log" --exclude "*/.hoodie/archive/*" --exclude "*/.hoodie/*.commit*" --exclude "*/.hoodie/*.clean*" --exclude "*/.hoodie/*.inflight*" --profile saml

