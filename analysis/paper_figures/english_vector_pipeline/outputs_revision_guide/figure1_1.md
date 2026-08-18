# Figure 1. Mind-Echo revision sample structure

Figure 1 is represented as text and tables only. The full descriptive dataset contains 84 patient records, 131 visit segments, 109 paired GAD-7/PHQ-9 scale records, and 5,531 role-labeled dialogue turns: 2,824 doctor turns, 1,889 caregiver turns, and 818 patient turns. Patient record is the unit for cohort and demographic descriptions; a paired scale record is a measurement unit; and dialogue turns are model inputs or features rather than independent analysis samples. Among the 84 descriptive patient records, 63 have at least one caregiver-labeled dialogue turn and 21 do not. Recorded age is available for 35 patient records, including 21 with pediatric ages from 0 through 17 years.

The revision machine-learning cohort is a distinct curated subset containing 78 visit-level prediction records nested within 63 patients. The visit is the primary machine-learning analysis and prediction unit, whereas patient_id is the grouping and resampling unit. The two counts of 63 describe different subsets: 63 descriptive patient records with caregiver-labeled turns and 63 patients represented in the curated machine-learning cohort. The available aggregate sources do not enumerate a single record-level exclusion reason for each difference between the full descriptive dataset and the ML cohort.

Five-fold GroupKFold cross-validation (CV) is performed using patient_id groups. The five test folds contain 16/16/16/15/15 visits from 13/13/13/12/12 patients. All visits from a patient remain in one fold, each of the 78 visits receives one out-of-fold (OOF) prediction, and performance metrics are computed by pooling the 78 OOF predictions. Ninety-five percent confidence intervals use 2,000 patient-level bootstrap draws. Anxiety is labeled positive at GAD-7 >= 10 (50 positive and 28 negative visits), and depression is labeled positive at PHQ-9 >= 10 (59 positive and 19 negative visits).

The source structure does not contain caregiver_id, family_id, recording_id, an EMR/MRN field, or a standalone scale_id. Caregiver status is derived from dialogue turn roles, no caregiver- or family-grouped CV is claimed, and scale respondent identity cannot be independently verified.

| Section | Measure | Count | Denominator | Note |
| --- | --- | --- | --- | --- |
| Full descriptive dataset | Patient records | 84 |  | Patient record is the unit for cohort and demographic descriptions. |
| Full descriptive dataset | Visit segments | 131 | 84 | Visits are nested within patient records. |
| Full descriptive dataset | Role-labeled dialogue turns | 5531 | 131 | Turns are model inputs or features, not independent analysis samples. |
| Full descriptive dataset | Doctor-labeled turns | 2824 | 5531 | Role-labeled turn count. |
| Full descriptive dataset | Caregiver-labeled turns | 1889 | 5531 | Role-labeled turns do not provide a caregiver or family identifier. |
| Full descriptive dataset | Patient-labeled turns | 818 | 5531 | Role-labeled turn count. |
| Clinical scales | Paired GAD-7/PHQ-9 scale records | 109 | 84 | A scale record is a measurement unit, not a patient count; respondent identity cannot be independently verified. |
| Caregiver turns | Patient records with at least one caregiver-labeled turn | 63 | 84 | Descriptive role-derived subset; distinct from the 63 patients in the ML cohort. |
| Caregiver turns | Patient records without caregiver-labeled turns | 21 | 84 | Descriptive patient records with no caregiver-labeled dialogue turn. |
| Age availability | Patient records with any recorded age | 35 | 84 | Age availability is incomplete. |
| Age availability | Patient records with pediatric age 0-17 | 21 | 84 | Pediatric age uses recorded ages from 0 through 17 years. |
| Revision ML cohort | Curated visit-level prediction records | 78 | 131 | The visit is the primary ML analysis and prediction unit. |
| Revision ML cohort | Patients represented in the curated ML cohort | 63 | 84 | patient_id is the cross-validation grouping and bootstrap resampling unit. |
| Revision ML targets | Anxiety-positive visits (GAD-7 >= 10) | 50 | 78 | The remaining 28 visits are anxiety-negative. |
| Revision ML targets | Depression-positive visits (PHQ-9 >= 10) | 59 | 78 | The remaining 19 visits are depression-negative. |
| Cross-validation | Test visits by fold | 16/16/16/15/15 | 78 | Each visit receives exactly one out-of-fold prediction. |
| Cross-validation | Test patients by fold | 13/13/13/12/12 | 63 | All visits from one patient remain in the same fold. |
| Uncertainty estimation | Patient-level bootstrap draws | 2000 | 63 | Patients, rather than visits or turns, are resampled for 95% confidence intervals. |
