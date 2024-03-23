# Airflow Project for EKS with Iceberg Tables Support

This repository contains an Apache Airflow project designed to run on an Amazon EKS (Elastic Kubernetes Service) cluster, with support for working with Iceberg tables. Iceberg is an open table format for large, slow-moving tabular datasets. It provides efficient data access and management for big data workloads.

## Prerequisites

- An AWS account with permissions to create and manage EKS clusters.
- Docker installed on your local machine for building Airflow images.
- `kubectl` configured to interact with your EKS cluster.
- Apache Airflow installed on your local machine or within the EKS cluster.

## Getting Started

1. **Clone the Repository**

   ```
   git clone https://github.com/mouradap/airflow-cluster-eks-iceberg.git
   cd airflow-cluster-eks-iceberg
   ```

2. **Working locally**

   Run docker cluster locally with docker-compose

   ```
   docker-compose up
   ```

## Contributing

Contributions to this project are welcome. Please follow the standard GitHub workflow:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Commit your changes and push to your fork.
4. Open a pull request against the main branch.

## License

This project is licensed under the Apache License 2.0. See the `LICENSE` file for details.

## Contact

For any questions or support, please open an issue on this GitHub repository.
