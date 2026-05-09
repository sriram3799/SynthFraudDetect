# --------------------------------------------------------------------------- #
# Bastion security group — SSH only from admin CIDR
# --------------------------------------------------------------------------- #

resource "aws_security_group" "bastion" {
  name        = "${var.project_name}-bastion-sg-${var.environment}"
  description = "Bastion host: SSH from admin CIDR only"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "SSH from admin CIDR"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.admin_cidr]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-bastion-sg-${var.environment}"
  }
}

# --------------------------------------------------------------------------- #
# Kafka security group — Kafka port from Flink SG only; SSH from bastion only
# --------------------------------------------------------------------------- #

resource "aws_security_group" "kafka" {
  name        = "${var.project_name}-kafka-sg-${var.environment}"
  description = "Kafka broker: port 9092 from Flink nodes, SSH from bastion"
  vpc_id      = aws_vpc.main.id

  ingress {
    description     = "Kafka plaintext from Flink nodes"
    from_port       = 9092
    to_port         = 9092
    protocol        = "tcp"
    security_groups = [aws_security_group.flink.id]
  }

  ingress {
    description     = "SSH from bastion"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.bastion.id]
  }

  egress {
    description = "All outbound"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-kafka-sg-${var.environment}"
  }
}

# --------------------------------------------------------------------------- #
# Flink security group — inter-node RPC, Web UI from bastion, SSH from bastion
# --------------------------------------------------------------------------- #

resource "aws_security_group" "flink" {
  name        = "${var.project_name}-flink-sg-${var.environment}"
  description = "Flink nodes: RPC + Web UI accessible from bastion; inter-node all"
  vpc_id      = aws_vpc.main.id

  ingress {
    description = "Flink RPC (inter-node)"
    from_port   = 6123
    to_port     = 6123
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description = "Flink data port range (inter-node)"
    from_port   = 6124
    to_port     = 6130
    protocol    = "tcp"
    self        = true
  }

  ingress {
    description     = "Flink Web UI from bastion"
    from_port       = 8081
    to_port         = 8081
    protocol        = "tcp"
    security_groups = [aws_security_group.bastion.id]
  }

  ingress {
    description     = "SSH from bastion"
    from_port       = 22
    to_port         = 22
    protocol        = "tcp"
    security_groups = [aws_security_group.bastion.id]
  }

  egress {
    description = "All outbound (S3, CloudWatch, NAT)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_name}-flink-sg-${var.environment}"
  }
}
