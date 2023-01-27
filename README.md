# Insurance Claims Workflow

[![DBR](https://img.shields.io/badge/DBR-12.1-red?logo=databricks&style=for-the-badge)](https://docs.databricks.com/release-notes/runtime/12.1.html)
[![CLOUD](https://img.shields.io/badge/CLOUD-AWS-orange?logo=googlecloud&style=for-the-badge)](https://databricks.com/try-databricks)
[![POC](https://img.shields.io/badge/POC-1_day-green?style=for-the-badge)](https://databricks.com/try-databricks)

Claims transformation is at the heart of many digital transformation programs within the insurance industry. These programs
are all aimed at creating a platform capable of automating as much of the entire claims workflow as possible. This repository
provides a sample project to showcase how an insurer could go about automating the data ingesting, processing and delivery
process.

## Table of Contents

* [Getting Started](#getting-started)
* [Infrastructure](#infrastructure)
* [Data Processing](#data-processing)
* [Deployment](#deployment)
  * [Local](#local)
  * [Cloud](#cloud)
* [Contributing](#contributing)

## Getting Started

The example project was designed around the [Databricks Lakehouse](https://www.databricks.com/product/data-lakehouse) 
architecture. A schematic of the reference architecture is shown below:

<div style="text-align: center">
  <img src="https://github.com/databricks-industry-solutions/dlt-insurance-claims/blob/main/assets/images/reference_architecture.png?raw=true" alt="Reference Architecture" width="100%">
</div>

The core architecture comprises four key layers: the data sources, an ingestion plan, the data processing layer, and 
finally a serving layer. In this project, we pay specific attention to the ingestion plan and the data processing layers.
The project provides a working example of how an organisation could use [Fivetran](https://www.fivetran.com/) to ingest 
batches of data, and then use the [Databricks Delta Live Tables](https://www.databricks.com/product/delta-live-tables)
service to construct and manage a simplified ETL workflow.

For the serving layer, we use [Databricks SQL](https://www.databricks.com/product/databricks-sql) to take care of ad hoc
queries and scheduled analytical workloads. The same service is used to power Databricks SQL Dashboards for prepackaged,
interactive reports and visualizations.

## Infrastructure

The technical infrastructure for this project relies on services from four providers: [Amazon Web Services (AWS)](https://aws.amazon.com/), 
[MongoDB Atlas](https://www.mongodb.com/atlas/database), [Fivetran](https://www.fivetran.com/), and
[Databricks](https://www.databricks.com/). The schematic for the technical architecture is shown below:

<div style="text-align: center">
  <img src="https://github.com/databricks-industry-solutions/dlt-insurance-claims/blob/main/assets/images/infrastructure_architecture.png?raw=true" alt="Reference Architecture" width="100%">
</div>

**NOTE**: Databricks supports all three major cloud providers. While the example project is built using services from
AWS, it could easily be reconstructed using comparable services offered by either [Microsoft Azure](https://azure.microsoft.com/), 
or [Google Cloud Platform (GCP)](https://cloud.google.com/).

The technical architecture is designed to emulate a basic workflow for the claims process of an automotive insurance 
provider. In this scenario, there are three data sources: 

  1) An enterprise data warehouse (EDW) containing policy records;
  2) An application database containing claims records; and
  3) A [BLOB](https://www.cloudflare.com/en-gb/learning/cloud/what-is-blob-storage/) store containing data purchased from 
a third-party provider.

The collection of data sources are emulated using services from AWS and MongoDB Atlas. Data is delivered to each source
at different intervals to simulate real-world conditions. For example, claims records are expected to update at 5-minute
intervals, while external data is only delivered by the provider at the end of every hour.

The infrastructure is configured, provisioned and managed using Terraform. The provider setup and resource configuration
for the entire architecture is available in the [`terraform`](./terraform) folder. The scripts have been separated into 
dedicated files for each of the respective service providers, e.g. the [`databricks.tf`](./terraform/databricks.tf) file
contains the configuration for all Databricks-related services.

## Data Processing

Data ingested from the three respective sources is delivered to the Databricks Lakehouse in [Delta Lake](https://delta.io/)-format 
tables using Fivetran. A [Delta Live Tables](https://www.databricks.com/product/delta-live-tables) pipeline is used to 
manage the ETL process and deliver curated and aggregated tables to the serving layer.

The ETL process is broken down into three steps: staging, curation, and aggregation. The outputs from these stages are 
classified as bronze, silver, and gold-level assets respectively (following the [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)).
The source code for each step is provided in separate Python files, and can be found under the [`notebooks/dlt`](./notebooks/dlt)
folder. The files are formatted for easy ingesting as [Databricks Notebooks](https://docs.databricks.com/notebooks/index.html)
within a Databricks workspace.

The complete set of sample data used for the project is made available under the [`data`](./data) folder.

## Deployment

The project is constructed to allow for deployment in either a local or cloud-based environment:

### Local

#### Dependencies

* Python (>=3.7)
* AWS Command Line Interface (>= 2.6.4)
* Terraform (>= 1.1.9)
* Docker (>= 20.10.20)
* MongoDB (>= 5.0.7)

#### Getting Started

The [`config.local`](./config.local) file contains a series of environment variables that has to be set before the infrastructure
can be deployed locally. Be sure to update the values of these variables before proceeding with the next steps. 

The local deployment requires running [MongoDB Community](https://www.mongodb.com/community) on the host machine. Specify
the following set of environment variables to configure the connection to the database:

```shell
TF_VAR_mongodb_host
TF_VAR_mongodb_username_readwrite
TF_VAR_mongodb_password_readwrite
TF_VAR_mongodb_username_read
TF_VAR_mongodb_password_read
```

Once the necessary environment variables have been configured, the infrastructure can be deployed using the standard
Terraform workflow and sequence of commands:

```shell
terraform init
terraform plan -out=plan.out
terraform apply --auto-approve plan.out
```

Be sure to have Docker running before trying to execute the above set of commands.

### Cloud

#### Getting Started

For the cloud deployment, specify the following set of environment variables to configure the connection to the MongoDB 
database cluster:

```shell
MONGODB_ATLAS_PUBLIC_KEY
MONGODB_ATLAS_PRIVATE_KEY
```

and

```shell
TF_VAR_mongodbatlas_org_id
TF_VAR_mongodbatlas_api_key_id
```

The cloud deployment requires setting up and configuring an Atlas Administration API. Please refer to the 
[official documentation](https://www.mongodb.com/docs/atlas/api/) for more information.

The infrastructure can be deployed using the same sequence of Terraform commands as that specified for the local deployment 
above.

## Destruction

The entire stack can be torn down using the standard Terraform workflow and commands:

```shell
terraform destroy --auto-approve
```

The Databricks SQL Warehouse can be removed by executing the commands given in the [`destroy.sql`](./setup/sql/databricks/destroy.sql)
file available under the [`setup/sql/databricks`](./setup/sql/databricks) folder. The commands can be executed from within 
the Databricks SQL query editor.

## Contributing

We value any feedback from the broader community. We welcome contributions of any kind, whether it's a bug report, new 
feature, fixes, or updates to existing documentation.  Please read through the [CONTRIBUTING](/CONTRIBUTING.md) document 
before submitting any issues or pull requests to ensure that we have all the necessary information, and can respond properly 
to your contribution.


___

&copy; 2023 Databricks, Inc. All rights reserved. The source in these notebooks is provided subject to the 
[Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are 
subject to the licenses set 
forth below.
