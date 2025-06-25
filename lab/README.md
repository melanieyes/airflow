![Foundry Data & AI Academy Logo](https://raw.githubusercontent.com/foundry-ai-academy/fa-cdn/1.0.0/images/FoundryAI_academy_logo_on_yellow_space.png)

# Lesson Plan: Airflow Orchestration on Snowflake

## Objective

Students will learn how to use Apache Airflow to orchestrate data workflows on Snowflake, focusing on authentication, task dependencies, dynamic task generation, and notifications.

## Exercise Overview

This lab exercise consists of five tasks:

1. Airflow Variable: Authenticate with Snowflake - Use Airflow variables to store credentials and create a table.

2. Airflow Connection: Insert data using Airflow Connection - Shift to using Airflow’s built-in connection manager.

3. Airflow Sensor: Implement a sensor - Ensure table exists before inserting data.

4. Notification: send a discord message using webhook upon success (and failure) - Integrate notifications into the @dag API.

5. Dag design: Putting all tasks together and test run.

There are 2 dags files already created: `lab_dag.py` and `lab_prep.py`. Your task is to follow
intructions detailed as TODOs in `lab_dag.py`. DO NOT make any change `lab_prep.py` as it's used to create 
data file that triggers sensor in `lab_dag`.

Once you finished with airflow sensor (task #3) you can trigger `lab_prep` from the Airflow UI.

---
© 2024 Foundry Data & AI Academy.  
All rights reserved.  
This material is confidential and proprietary to Foundry Data & AI Academy. It may not be reproduced, transmitted, or stored, in whole or in part, in any form or by any means without written permission from Foundry Data & AI Academy.

![Foundry Data & AI Academy Logo](https://raw.githubusercontent.com/foundry-ai-academy/fa-cdn/1.0.0/images/FoundryAI_academy_logo_symbol_yellow_space.png)