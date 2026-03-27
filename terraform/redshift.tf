# Redshift — analytical OLAP data warehouse.
# Placed in private subnets with encryption enabled.

resource "aws_redshift_subnet_group" "main" {
  name       = "${local.name_prefix}-redshift-subnet-group"
  subnet_ids = [aws_subnet.private_1.id, aws_subnet.private_2.id]
  tags       = { Name = "${local.name_prefix}-redshift-subnet-group" }
}

resource "aws_redshift_cluster" "main" {
  cluster_identifier = "${local.name_prefix}-redshift-${local.name_suffix}"

  node_type       = var.redshift_node_type
  number_of_nodes = var.redshift_number_of_nodes
  cluster_type    = var.redshift_number_of_nodes > 1 ? "multi-node" : "single-node"

  database_name   = "analytics"
  master_username = var.db_username
  master_password = var.db_password
  port            = 5439

  cluster_subnet_group_name = aws_redshift_subnet_group.main.name
  vpc_security_group_ids    = [aws_security_group.redshift.id]
  publicly_accessible       = false
  enhanced_vpc_routing      = true
  encrypted                 = true

  automated_snapshot_retention_period = 1
  skip_final_snapshot                 = true
  preferred_maintenance_window        = "sun:05:00-sun:06:00"
  allow_version_upgrade               = true

  tags = { Name = "${local.name_prefix}-redshift" }
}
