# ECS Fargate deployment for the Streamlit analytics dashboard.
# ALB on port 80 forwards to Streamlit on port 8501.

# ── ECR Repository ─────────────────────────────────────

resource "aws_ecr_repository" "dashboard" {
  name                 = "${local.name_prefix}-dashboard"
  image_tag_mutability = "MUTABLE"
  force_delete         = true

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = { Name = "${local.name_prefix}-dashboard-ecr" }
}

# ── ECS Cluster ────────────────────────────────────────

resource "aws_ecs_cluster" "dashboard" {
  name = "${local.name_prefix}-dashboard-cluster"
  tags = { Name = "${local.name_prefix}-dashboard-cluster" }
}

# ── CloudWatch Log Group ───────────────────────────────

resource "aws_cloudwatch_log_group" "dashboard" {
  name              = "/ecs/${local.name_prefix}-dashboard"
  retention_in_days = 7
  tags              = { Name = "${local.name_prefix}-dashboard-logs" }
}

# ── IAM — Task Execution Role ─────────────────────────

resource "aws_iam_role" "ecs_task_execution" {
  name = "${local.name_prefix}-ecs-exec-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })

  tags = { Name = "${local.name_prefix}-dashboard-exec-role" }
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# ── IAM — Task Role (app permissions) ─────────────────

resource "aws_iam_role" "ecs_task" {
  name = "${local.name_prefix}-ecs-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })

  tags = { Name = "${local.name_prefix}-dashboard-task-role" }
}

resource "aws_iam_policy" "ecs_task" {
  name        = "${local.name_prefix}-ecs-task-policy"
  description = "Grants dashboard S3 read access for streaming monitor"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3ReadAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.data.arn,
          "${aws_s3_bucket.data.arn}/*"
        ]
      }
    ]
  })

  tags = { Name = "${local.name_prefix}-dashboard-task-policy" }
}

resource "aws_iam_role_policy_attachment" "ecs_task" {
  role       = aws_iam_role.ecs_task.name
  policy_arn = aws_iam_policy.ecs_task.arn
}

# ── Security Groups ───────────────────────────────────

resource "aws_security_group" "alb" {
  name        = "${local.name_prefix}-dashboard-alb-sg"
  description = "Allow HTTP to dashboard ALB"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "HTTP from anywhere"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-dashboard-alb-sg" }
}

resource "aws_security_group" "ecs_task" {
  name        = "${local.name_prefix}-dashboard-ecs-sg"
  description = "Allow Streamlit traffic from ALB only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "Streamlit from ALB"
    from_port       = 8501
    to_port         = 8501
    protocol        = "tcp"
    security_groups = [aws_security_group.alb.id]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${local.name_prefix}-dashboard-ecs-sg" }
}

# Allow ECS tasks to reach Redshift
resource "aws_security_group_rule" "redshift_from_ecs" {
  type                     = "ingress"
  description              = "Redshift from dashboard ECS tasks"
  from_port                = 5439
  to_port                  = 5439
  protocol                 = "tcp"
  source_security_group_id = aws_security_group.ecs_task.id
  security_group_id        = aws_security_group.redshift.id
}

# ── Application Load Balancer ─────────────────────────

resource "aws_lb" "dashboard" {
  name               = "${local.name_prefix}-dash-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.alb.id]
  subnets            = [aws_subnet.public_1.id, aws_subnet.public_2.id]

  tags = { Name = "${local.name_prefix}-dashboard-alb" }
}

resource "aws_lb_target_group" "dashboard" {
  name        = "${local.name_prefix}-dash-tg"
  port        = 8501
  protocol    = "HTTP"
  vpc_id      = aws_vpc.main.id
  target_type = "ip"

  health_check {
    path                = "/_stcore/health"
    port                = "traffic-port"
    healthy_threshold   = 2
    unhealthy_threshold = 3
    timeout             = 10
    interval            = 30
    matcher             = "200"
  }

  tags = { Name = "${local.name_prefix}-dashboard-tg" }
}

resource "aws_lb_listener" "dashboard" {
  load_balancer_arn = aws_lb.dashboard.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.dashboard.arn
  }

  tags = { Name = "${local.name_prefix}-dashboard-listener" }
}

# ── ECS Task Definition ───────────────────────────────

resource "aws_ecs_task_definition" "dashboard" {
  family                   = "${local.name_prefix}-dashboard"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task.arn

  container_definitions = jsonencode([{
    name      = "dashboard"
    image     = "${aws_ecr_repository.dashboard.repository_url}:latest"
    essential = true

    portMappings = [{
      containerPort = 8501
      protocol      = "tcp"
    }]

    environment = [
      { name = "REDSHIFT_HOST",     value = replace(aws_redshift_cluster.main.endpoint, ":5439", "") },
      { name = "REDSHIFT_PORT",     value = "5439" },
      { name = "REDSHIFT_DATABASE", value = aws_redshift_cluster.main.database_name },
      { name = "REDSHIFT_USER",     value = var.db_username },
      { name = "REDSHIFT_PASSWORD", value = var.db_password },
      { name = "S3_BUCKET",         value = aws_s3_bucket.data.id },
      { name = "S3_PREFIX",         value = "streaming/" },
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.dashboard.name
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = "dashboard"
      }
    }
  }])

  tags = { Name = "${local.name_prefix}-dashboard-task" }
}

# ── ECS Service ────────────────────────────────────────

resource "aws_ecs_service" "dashboard" {
  name            = "${local.name_prefix}-dashboard-svc"
  cluster         = aws_ecs_cluster.dashboard.id
  task_definition = aws_ecs_task_definition.dashboard.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = [aws_subnet.private_1.id, aws_subnet.private_2.id]
    security_groups  = [aws_security_group.ecs_task.id]
    assign_public_ip = false
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.dashboard.arn
    container_name   = "dashboard"
    container_port   = 8501
  }

  depends_on = [aws_lb_listener.dashboard]

  tags = { Name = "${local.name_prefix}-dashboard-svc" }
}
