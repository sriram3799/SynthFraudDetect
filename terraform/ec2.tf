data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# --------------------------------------------------------------------------- #
# Bastion host — public subnet, minimal instance
# --------------------------------------------------------------------------- #

resource "aws_instance" "bastion" {
  ami                         = data.aws_ami.amazon_linux_2023.id
  instance_type               = "t3.micro"
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.bastion.id]
  associate_public_ip_address = true

  root_block_device {
    volume_size           = 8
    volume_type           = "gp3"
    delete_on_termination = true
    encrypted             = true
  }

  tags = {
    Name = "${var.project_name}-bastion-${var.environment}"
    Role = "bastion"
  }
}

# --------------------------------------------------------------------------- #
# Kafka broker — private AZ-a, same AZ as Flink to eliminate cross-AZ hops
# --------------------------------------------------------------------------- #

resource "aws_instance" "kafka" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = "m5.xlarge"          # 4 vCPU / 16 GB — matches Docker limit
  subnet_id              = aws_subnet.private_a.id
  vpc_security_group_ids = [aws_security_group.kafka.id]
  iam_instance_profile   = aws_iam_instance_profile.flink.name
  availability_zone      = aws_subnet.private_a.availability_zone   # explicit AZ lock

  root_block_device {
    volume_size           = 100
    volume_type           = "gp3"
    throughput            = 250
    iops                  = 3000
    delete_on_termination = true
    encrypted             = true
  }

  user_data = base64encode(<<-EOF
    #!/bin/bash
    yum update -y
    yum install -y docker
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user
  EOF
  )

  tags = {
    Name = "${var.project_name}-kafka-${var.environment}"
    Role = "kafka"
  }
}

# --------------------------------------------------------------------------- #
# Flink nodes (2×) — private AZ-a, same as Kafka
# --------------------------------------------------------------------------- #

resource "aws_instance" "flink" {
  count = 2

  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = "c5.2xlarge"         # 8 vCPU / 16 GB — matches Docker limit
  subnet_id              = aws_subnet.private_a.id
  vpc_security_group_ids = [aws_security_group.flink.id]
  iam_instance_profile   = aws_iam_instance_profile.flink.name
  availability_zone      = aws_subnet.private_a.availability_zone   # explicit AZ lock

  root_block_device {
    volume_size           = 100
    volume_type           = "gp3"
    throughput            = 250
    iops                  = 3000
    delete_on_termination = true
    encrypted             = true
  }

  user_data = base64encode(<<-EOF
    #!/bin/bash
    yum update -y
    yum install -y docker
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user
  EOF
  )

  tags = {
    Name = "${var.project_name}-flink-${count.index}-${var.environment}"
    Role = "flink"
  }
}
