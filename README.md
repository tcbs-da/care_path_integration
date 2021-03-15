# Care_Path_Integration

# Basic Logic:
## 1. Clean and reform raw EMR data by features, "Key", "ID", "Diag", "Treatment", "Office", into sensible first-visit dataset and subsequent-visit datasets 
## 2. Refill 'Unknown' Treatment following first-visit's
## 3. Join consistent first and subsequent datasets by features, "Key", "ID", "Diag", "Treatment", "Office"

# Instruction:
### import care_pathway_integration as cpi
### cpi.care_pathway(dataframe, process_number) #### (dataframe is the raw data, process_number is the number of process you want to use)
### linked = clean.pathway_link()
