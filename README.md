# Mind-Echo（数智心晴）
[简体中文](README_CN.md) | [English](README.md)

[![Project](https://img.shields.io)](https://www.fudan.edu.cn)
[![License](https://img.shields.io)](https://opensource.org)
## Project Introduction
This project is based on the Fudan University Xiyuan Program, focusing on a series of studies regarding the quantitative psychological characteristics (anxiety and depression indices) of outpatients. This repository showcases our team's research process and achievements. Specifically:
1. We shadowed a large number of patients in the ENT (Ear, Nose, and Throat) outpatient department of relevant hospitals and created the ZZHL Anxiety and Depression Doctor-Patient Dialogue Dataset Demo. The dataset contains 133 dialogues between patients and doctors in outpatient settings, covering both initial and follow-up consultations to achieve maximum scenario coverage. For every patient collected, they agreed to fill out the PHQ-9 and GAD-7 questionnaires to measure anxiety and depression indicators after the consultation. After data cleaning, we are open-sourcing this work, which is currently one of the few datasets in China related to psychological anxiety and depression indices in outpatient medical settings.
2. We conducted a comparative study on the suitability of different AI methods for emotional risk identification in outpatient medical scenarios. Specifically, we compared zero-shot learning, few-shot learning + feature engineering, and traditional feature engineering + machine learning schemes. Meanwhile, based on existing Medical Large Language Models (LLMs), we briefly compared the LoRA fine-tuning scheme with a scheme that uses VADER, Personality, and LIWC-CH to extract patients' linguistic features, incorporates these quantitative indicators into prompts, and then provides them to the medical LLM for prediction.
3. As a medical study, we researched the real-time trends of psychological indices during the outpatient stage, using models to monitor at which stage parents' anxiety levels suddenly rise. We manually segmented outpatient records (including initial and follow-up stages) and divided the same consultation into three phases: Chief Complaint, Notification, and Consultation. Finally, we produced time-series line charts with the dialogue progress on the horizontal axis and the anxiety score on the vertical axis, attempting to find the impact of doctors' speech on patients' anxiety. Simultaneously, we conducted a comparative study on the correlation between linguistic features and demographic characteristics (such as urban-rural differences) among patients in different psychological feature intervals.
4. Regarding the project, we ultimately developed an Intelligent Identification System for Anxiety and Depression tailored for the outpatient environment.
**Specially**, I developed a data processing pipeline for the complex characteristics of medical scenarios, directly implementing a series of processes from natural language extraction to natural language processing. It mines outpatients' psychological quantitative features in real-time and utilizes machine learning/deep learning models to achieve automated and precise identification of anxiety and depression risks. In the future, we will consider using Agent workflows to integrate other related work into our pipeline.

## Research Background

## Technical Architecture
Based on fine-tuned Chinese medical large language models (BERT/GPT categories), the system integrates multi-modal feature engineering (such as acoustics and localized LIWC dictionaries), combined with few-shot learning and expert rule refinement. We have optimized model inference speed and ensured data security and compliance through privacy de-identification mechanisms (converting speech into feature vectors/abstract labels).

## Quantitative Metrics
Core scales (PHQ-9 and GAD-7) are utilized to provide a scientific basis for the research.

## Citation
## Ethical Statement
This project is for scientific research purposes only. All data have been de-identified and do not involve any personal privacy of patients.Data is anonymized and adheres to ethical standards.
