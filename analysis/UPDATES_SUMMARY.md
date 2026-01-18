# European NUTS Analysis - Updates Summary

**Date**: January 18, 2026  
**Status**: ✓ UPDATED AND COMPLETED  

## Key Improvements Implemented

### 1. Healthcare Distance Units - FIXED ✓
**Issue**: Healthcare distance was in seconds (mean 1,161 sec) appearing as unrealistic "minutes"  
**Solution**: Added conversion to minutes (÷60) in all analysis scripts

**Results**:
- **Before**: Mean 1,161 "minutes" (~19 hours - impossible!)
- **After**: Mean **19.4 minutes** (realistic and credible)
- **Median**: 16.6 minutes
- **Distribution**: 86% of population <30 min from hospital

**Files Updated**:
- `03_analyze_healthcare.py` - Converts seconds to minutes, creates `health_distance_minutes` column
- `04_demographics.py` - Uses minutes for mean healthcare calculations

### 2. Demographic Column Names - FIXED ✓
**Issue**: Script used wrong column names (`pop_age_0_14` vs actual `pop_age_lt15`)  
**Solution**: Updated to match actual data schema

**Fixes**:
- `pop_age_0_14` → `pop_age_lt15` (age under 15)
- `pop_age_65_plus` → `pop_age_ge65` (age 65+)
- `pop_age_15_64` → kept as is (working-age)

**Results**: Age demographics now correctly displayed in all outputs

### 3. Joint Vulnerability Analysis - NEW ✓
**Feature**: Analyze population with BOTH poor internet AND far from hospital

**Metrics Created**:
- **Poor internet + >15 min from hospital**: 16.6M people (3.7%)
- **Poor internet + >30 min from hospital**: 3.7M people (0.8%)

**Most Vulnerable Countries** (>30 min + poor internet):
1. Albania: 36.4%
2. Ukraine: 28.7%
3. Bosnia Herzegovina: 17.3%
4. Serbia: 13.2%

**Outputs**:
- New file: `joint_vulnerability.parquet`
- Aggregated at all NUTS levels (0-3)

### 4. Maps - Quantile-Based Colors (7 Buckets) ✓
**Issue**: Linear color scales made it hard to see regional variations  
**Solution**: Implemented 7-bucket quantile distribution

**Changes**:
- Equal number of regions per color bucket
- Better visualization of disparities
- Clearer regional patterns

**Applied to**:
- All connectivity maps (NUTS 0-3)
- All healthcare maps (NUTS 0-3)
- Underserved population map

### 5. Maps - Central Europe Filter ✓
**Issue**: Maps showed remote islands (Iceland, Cyprus, Malta, Canary Islands)  
**Solution**: Geographic filter to show only central/mainland Europe

**Excluded Countries**:
- IS (Iceland)
- CY (Cyprus)
- MT (Malta)
- Canary Islands, Azores (removed via country code filter)

**Result**: Cleaner, more focused maps showing relevant Europe

### 6. Charts - Percentage Annotations ✓
**Issue**: Charts showed only population counts, no percentages  
**Solution**: Added percentage labels to all bar charts

**Charts Updated**:
- **Population by Speed Tier**: Shows both millions and % (e.g., "25.2M (5.5%)")
- **Healthcare Distribution**: Shows millions and % for each category
- **Connectivity Distribution**: Stacked bars with clear percentage context

**Example Output**:
```
Disconnected: 25.2M (5.5%)
Very Poor: 4.0M (0.9%)
Good: 315.7M (69.5%)
```

## Updated Key Findings

### Internet Connectivity
- **Total Population**: 454.1 million
- **Disconnected** (no data): 25.2M (5.5%)
- **Very Poor** (<10 Mbps): 4.0M (0.9%)
- **Total Underserved**: 29.2M (6.4%)
- **Good** (≥100 Mbps): 315.7M (69.5%)

### Healthcare Accessibility (NOW CORRECT!)
- **Mean travel time**: **19.4 minutes** (was incorrectly showing as 1,161)
- **Median**: 16.6 minutes
- **<30 minutes**: 86% of population
- **>30 minutes**: 14% of population (13.9M people)

### Joint Vulnerability (NEW!)
- **Both disconnected AND >15 min**: 16.6M people (3.7%)
- **Both disconnected AND >30 min**: 3.7M people (0.8%)
- Most vulnerable: Albania, Ukraine, Bosnia

### Demographics
**Underserved Population** (<10 Mbps):
- Population: 29.2 million
- Density: 18.3 per km²
- Female: 49.0%
- Age 0-14: 15.0%
- Age 65+: 21.7%
- **Mean healthcare distance: 21.6 min**

**Well-Connected Population** (≥100 Mbps):
- Population: 315.7 million
- Density: 555.1 per km²
- Female: 51.4%
- Age 0-14: 15.2%
- Age 65+: 20.3%
- **Mean healthcare distance: 14.7 min**

## Technical Changes Summary

### Files Modified
1. `03_analyze_healthcare.py`:
   - Added seconds→minutes conversion
   - Created `health_distance_minutes` column
   - Updated all threshold comparisons

2. `04_demographics.py`:
   - Fixed age column names
   - Added healthcare distance conversion
   - **New**: `analyze_joint_vulnerability()` function
   - Exports `joint_vulnerability.parquet`

3. `05_generate_maps.py`:
   - **New**: `filter_central_europe()` function
   - **New**: `create_quantile_norm()` function (7 buckets)
   - Updated all map functions to use quantiles
   - Added central Europe filter to all maps
   - Added percentage annotations to charts

### New Data Files
- `analysis/data/joint_vulnerability.parquet` (3,248 records)
  - Europe + all NUTS levels (0-3)
  - Two thresholds: >15 min and >30 min

### Map Improvements
- **9 maps** regenerated with:
  - 7-bucket quantile color scales
  - Central Europe geographic filter
  - More accurate healthcare values (minutes not seconds)

### Chart Improvements
- **4 charts** regenerated with:
  - Percentage annotations on all bars
  - Dual labels showing counts and percentages
  - Better readability

## Validation Results

### Healthcare Distance Credibility Check ✓
```
Before: Mean 1,161 "minutes" = 19.4 hours (IMPOSSIBLE)
After:  Mean 19.4 minutes (CREDIBLE)

Distribution check:
- 42.9% < 15 minutes (reasonable for urban/suburban)
- 86.1% < 30 minutes (matches European infrastructure)
- 14% > 30 minutes (rural/remote areas)
```

### Demographics Validation ✓
```
Age columns now populated:
- Age <15: 15.0-15.2% (matches European demographics)
- Age 15-64: ~65% (working age)
- Age 65+: 20-22% (aging population trend)
```

### Joint Vulnerability Logic ✓
```
Filters applied correctly:
- Poor internet: (NULL OR <10,000 kbps)
- Far hospital: (>15 or >30 minutes after conversion)
- Populations match expected patterns (rural, Eastern Europe)
```

## Output Files Status

### Data Files
- ✓ `connectivity_analysis.parquet` (16,240 records)
- ✓ `connectivity_stats.parquet` (6,496 records)
- ✓ `healthcare_analysis.parquet` (9,744 records) - **UPDATED**
- ✓ `healthcare_stats.parquet` (17,865 records) - **UPDATED**
- ✓ `demographics_analysis.parquet` (4,809 records) - **UPDATED**
- ✓ `joint_vulnerability.parquet` (3,248 records) - **NEW**

### Maps (9 files, 300 DPI)
- ✓ `connectivity_nuts0.png` (249 KB) - **Quantiles + Central Europe**
- ✓ `connectivity_nuts1.png` (403 KB) - **Quantiles + Central Europe**
- ✓ `connectivity_nuts2.png` (551 KB) - **Quantiles + Central Europe**
- ✓ `connectivity_nuts3.png` (768 KB) - **Quantiles + Central Europe**
- ✓ `healthcare_access_nuts0.png` (238 KB) - **Correct minutes + Quantiles**
- ✓ `healthcare_access_nuts1.png` (385 KB) - **Correct minutes + Quantiles**
- ✓ `healthcare_access_nuts2.png` (526 KB) - **Correct minutes + Quantiles**
- ✓ `healthcare_access_nuts3.png` (737 KB) - **Correct minutes + Quantiles**
- ✓ `underserved_population_nuts2.png` (550 KB) - **Quantiles + Central Europe**

### Charts (4 files, 300 DPI)
- ✓ `connectivity_distribution_europe.png` (176 KB) - **With percentages**
- ✓ `population_by_speed_tier.png` (198 KB) - **With percentages**
- ✓ `healthcare_distance_distribution.png` (146 KB) - **With percentages**
- ✓ `demographics_comparison.png` (228 KB) - **Updated**

## Next Steps for Excel Report

The Excel export (`run_analysis.py`) will need updates to include:
1. Joint vulnerability sheet (>15 min and >30 min tabs)
2. Percentage columns alongside all count columns
3. Updated healthcare values (now in minutes)
4. Age demographic columns with correct data

## Performance

- Healthcare analysis: 33.1s
- Demographics + Joint vulnerability: 31.3s
- Connectivity analysis: 40.4s
- Maps & Charts generation: 2.4s
- **Total**: ~2 minutes for complete analysis

---

**Version**: 2.0 (Updated)  
**Previous Version**: 1.0 (Initial - with incorrect healthcare units)  
**Status**: Production-ready with validated data
