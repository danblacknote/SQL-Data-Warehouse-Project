# SQL-Data-Warehouse-Project
Building a full functional data warehouse project demonstrates a data warehouse project for actionable insight and analytical solution,  using SQL from building a data warehouse to generating actionable insight that includes ETL Processing, Data Modeling, and Analytic Metrics


Overview

* This project involves:

. Data Architecture: Designing a modern data warehouse  using a modaling architecture, Bronze, Silver, Gold Layers
. ETL Processing: Extracting, Transforming, and Loading data from source to warehouse
. Data Modeling: Developing Dimension and Fact Tables that are optimized for analytics and other queries
. Analytic and Reporting: Create SQL-based reports and dashboards for actionable insights


* This Repository is an excellent resource for anyone who wants to showcase expertise in:

. SQL-Development
. Data Architecture 
. Data Engineering
. ETL Pipeline Development 
. Data Modeling
. Data Analytics 


-> Important Tools 

. SQL Server                       
. SQL Server Management Studio     
. DrawIO                         
. Notion                           


* Project Requirements

 Building Data Warehouse (Data Engineering)

  Objectives
  
Develop a Modern Data Warehouse Using SQL to consolidate sales data, enabling
analytical reporting and informed decision-making.


  Specification

  . Data Source: Import data from two different sources (CRM and ERP) provided as CSV file
  . Data Quality: Clean, resolve, and  transform data quality issues before analytics 
  . Integration: Combine both sources into a single user-friendly data model designed for analytics
  . Scopr: Focus on the latest dataset only; historization of data is or required 
  . Documentation: Provide clear documentation of the data model to support both stakeholders and the analytics team


  
Data Architeture 


![image alt](https://github.com/danblacknote/SQL-Data-Warehouse-Project/blob/14d7488ab5af5cc29aea21db97bac4b3fc332ec5/docs/DWH%20Level%20of%20Architeture%20Project%20%20Diagram.jpg)


. Bronze Layer: Extract data from the source in row form. Data is ingested in CSV format into the SQL Database
. Silver Layer: This layer includes data cleaning, transforming, standardization, and normalization to prepare data 
for analytics 
. Gold Layer: It contains a business-ready data model in a star schema required for analytics and reporting 



Repository Structure 

-------- SQL-DatawarehuseProject

            -------datasets/                                          #Raw Dataset 
            |-------docs                                              # All the documents involving  this project
            |        |----- Data Flow Diagram. gpeg                   # Data flow diagram that shows how the data flows from its source to the warehouse 
            |        |----- Data Mart (Star Schema).gpeg              # Data Mart (Star Schema) diagram that represents data modaling between tables 
            |        |----- DWH Architecture Diagram. gpeg            # DWH Architecture diagram that represents the  whole architecture of the project  
            |        |----- Relationship Schema. gpeg                 # Relationship Diagram that shows the relationship of tables from our different sources
            |        |----- Data Cataloge.md                          # description of the column at the dimension and fact table of gold layers 
            
                       
            |-------scripts                                          # All SQL scripts used to extract, transform, and load data                                   
            |         |--- Bronze/                                   # SQL queries to extract and load data to the Bronze layer
            |         |--- Silver/                                   # SQL scripts used to clean and transform, and load data to the silver layer
            |         |--- Gold/                                     # SQL scripts used for analytical modeling
            |
            |--------tests/                                          #Test scripts used to ocheck the quality of the dat
            |
            |
            |
            |--------Readme.md                                       # Project Overview and instruction
            |--------.gitignore                                      # Files and directories to be ignored by Git


 Licencing
 This Project is licensed under the MIT license. You are free to use, modify, and share it with people



 About Me 
 Hey, I'm Deneke Zewdu, A Data Manager/Analyst/Web Developer, generally a passionate IT professional on a mission to make a difference
 using data and all the attributes it brings. I enjoy uncovering the patterns and insights using data, and I'm enthusiastic about 
 sharing my knowledge about the potential of data.
            










