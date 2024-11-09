# onprem-sql-mapping-system
A PL/SQL-based ETL process for mapping and reconciling data between two sources.

## Overview
The purpose of this ETL flow is to map data from two distinct systems, each with its own session close time, resulting in end-of-day unmatched data. This system reprocesses unmatched data by comparing it with data from the following day, allowing for a tolerance period. If records remain unmatched after this period, they are marked as conflict data.

## Structure
![alt text](https://github.com/annguyen-git/onprem-sql-mapping-system/blob/main/Doisoat.jpg)

## Key Features
- Tolerance Period for Data Reconciliation: Allows a specified number of days for data to match between systems.
- Conflict Resolution: Unmatched data past the tolerance period is marked as conflict.
- Optimized for PL/SQL: Fully implemented in PL/SQL for performance and ease of integration in an on-premise environment.

## Prerequisites
- Oracle Database with PL/SQL support.
- Permissions to run stored procedures and manage tables in the target database.
