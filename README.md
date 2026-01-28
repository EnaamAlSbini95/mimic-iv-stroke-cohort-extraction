# mimic-iv-stroke-cohort-extraction
Algorithms for extracting and balancing stroke patient cohorts from the MIMIC-IV critical care database.
This repository contains the code developed as part of a bachelor's thesis at KTH
focusing on the extraction and balancing of stroke patient cohorts from the
MIMIC-IV database.

## Methods
The repository includes implementations of:
- Propensity Score Stratification
- Nearest Neighbor Matching with caliper

## Data
Due to data use agreements, no patient-level data from MIMIC-IV is shared.
Users must obtain access to MIMIC-IV and run the SQL scripts locally.

## Reproducibility
All steps from cohort extraction to balance diagnostics are documented and
reproducible given appropriate database access.

## Requirements
See `requirements.txt`

## Disclaimer
This code is for research and educational purposes only.
