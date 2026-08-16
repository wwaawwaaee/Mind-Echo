# Figure 1. Mind-Echo revision sample structure

Figure 1 is represented as text and tables only. The data structure contains 84 patient records from 84 dialogue source files, 131 visit segments, and 109 paired GAD-7/PHQ-9 scale records. Among the 84 patient records, 63 have at least one caregiver-labeled dialogue turn and 21 do not. Recorded age is available for 35 patient records, including 21 with pediatric ages from 0 through 17 years. The revision machine-learning analyses use 78 visit-level records from 63 patients with patient-grouped five-fold OOF predictions.

The source structure does not contain caregiver_id, family_id, recording_id, an EMR/MRN field, or a standalone scale_id. Caregiver status is derived from dialogue turn roles, and scale respondent identity cannot be independently verified.

| Section | Measure | Count | Denominator | Note |
| --- | --- | --- | --- | --- |
| Dialogue source | Patient records from dialogue source files | 84 | 84 | One patient record per dialogue source file. |
| Dialogue source | Visit segments | 131 |  | Visit-level dialogue segments across patient records. |
| Clinical scales | Paired GAD-7/PHQ-9 scale records | 109 |  | Scale respondent identity cannot be independently verified. |
| Caregiver turns | Patient records with at least one caregiver-labeled turn | 63 | 84 | Caregiver status is role-derived from dialogue turns. |
| Caregiver turns | Patient records without caregiver-labeled turns | 21 | 84 | No caregiver-labeled dialogue turn was present. |
| Age availability | Patient records with any recorded age | 35 | 84 | Age availability is incomplete. |
| Age availability | Patient records with pediatric age 0-17 | 21 | 84 | Pediatric age uses recorded ages from 0 through 17 years. |
| Revision ML cohort | Visit-level records used in revision ML analyses | 78 |  | Patient-grouped five-fold OOF analyses. |
| Revision ML cohort | Patients used in revision ML analyses | 63 |  | Patient identifier is the grouping field. |
