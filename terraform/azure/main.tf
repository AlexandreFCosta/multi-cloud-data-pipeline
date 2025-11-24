# Azure Infrastructure for Multi-Cloud Data Pipeline
# This module creates essential Azure resources for data engineering

terraform {
  required_version = ">= 1.0"
  
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Variables
variable "project_name" {
  description = "Project name for resource naming"
  type        = string
  default     = "multicloud-pipeline"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "location" {
  description = "Azure region"
  type        = string
  default     = "East US"
}

variable "tags" {
  description = "Tags to apply to all resources"
  type        = map(string)
  default = {
    Project     = "Multi-Cloud Pipeline"
    ManagedBy   = "Terraform"
  }
}

# Resource Group
resource "azurerm_resource_group" "main" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location
  tags     = var.tags
}

# Storage Account for Data Lake Gen2
resource "azurerm_storage_account" "data_lake" {
  name                     = "st${replace(var.project_name, "-", "")}${var.environment}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled          = true  # Enable hierarchical namespace for Data Lake Gen2
  
  blob_properties {
    versioning_enabled = true
    
    delete_retention_policy {
      days = 7
    }
  }
  
  tags = var.tags
}

# Storage Containers
resource "azurerm_storage_container" "raw_data" {
  name                  = "raw-data"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "processed_data" {
  name                  = "processed-data"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "staging" {
  name                  = "staging"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

# Databricks Workspace
resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location
  sku                 = "premium"
  
  tags = var.tags
}

# Synapse Workspace
resource "azurerm_synapse_workspace" "main" {
  name                                 = "synw-${var.project_name}-${var.environment}"
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = azurerm_resource_group.main.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.synapse.id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = random_password.synapse_sql_password.result
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = var.tags
}

# Data Lake Gen2 Filesystem for Synapse
resource "azurerm_storage_data_lake_gen2_filesystem" "synapse" {
  name               = "synapse"
  storage_account_id = azurerm_storage_account.data_lake.id
}

# Random password for Synapse SQL admin
resource "random_password" "synapse_sql_password" {
  length  = 16
  special = true
}

# Synapse SQL Pool (optional - can be expensive)
resource "azurerm_synapse_sql_pool" "main" {
  name                 = "analytics"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  sku_name             = "DW100c"
  create_mode          = "Default"
  
  tags = var.tags
}

# Synapse Spark Pool
resource "azurerm_synapse_spark_pool" "main" {
  name                 = "sparkpool"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Small"
  
  auto_scale {
    max_node_count = 10
    min_node_count = 3
  }
  
  auto_pause {
    delay_in_minutes = 15
  }
  
  spark_version = "3.3"
  
  tags = var.tags
}

# Event Hub Namespace
resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-${var.project_name}-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 1
  
  tags = var.tags
}

# Event Hub
resource "azurerm_eventhub" "events" {
  name                = "user-events"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 7
}

# Data Factory
resource "azurerm_data_factory" "main" {
  name                = "adf-${var.project_name}-${var.environment}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  
  identity {
    type = "SystemAssigned"
  }
  
  tags = var.tags
}

# Key Vault for secrets
resource "azurerm_key_vault" "main" {
  name                       = "kv-${var.project_name}-${var.environment}"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false
  
  tags = var.tags
}

data "azurerm_client_config" "current" {}

# Outputs
output "resource_group_name" {
  description = "Resource group name"
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "Storage account name"
  value       = azurerm_storage_account.data_lake.name
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = azurerm_databricks_workspace.main.workspace_url
}

output "synapse_workspace_name" {
  description = "Synapse workspace name"
  value       = azurerm_synapse_workspace.main.name
}

output "synapse_sql_endpoint" {
  description = "Synapse SQL endpoint"
  value       = azurerm_synapse_workspace.main.connectivity_endpoints.sql
}

output "eventhub_namespace" {
  description = "Event Hub namespace"
  value       = azurerm_eventhub_namespace.main.name
}

output "data_factory_name" {
  description = "Data Factory name"
  value       = azurerm_data_factory.main.name
}

output "key_vault_uri" {
  description = "Key Vault URI"
  value       = azurerm_key_vault.main.vault_uri
}
