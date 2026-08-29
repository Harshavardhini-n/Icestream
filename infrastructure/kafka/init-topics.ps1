$ErrorActionPreference = "Stop"

$composeFile = Join-Path $PSScriptRoot "docker-compose.kafka.yml"
$serviceName = "kafka"

$requiredTopics = @(
    @{ Name = "checkout-events"; Partitions = 3; ReplicationFactor = 1 },
    @{ Name = "checkout-dlq"; Partitions = 3; ReplicationFactor = 1 },
    @{ Name = "icestream-control"; Partitions = 1; ReplicationFactor = 1 }
)

function Test-KafkaReady {
    $status = docker compose -f $composeFile ps --status running --services
    if ($LASTEXITCODE -ne 0) {
        return $false
    }

    if (-not $status -or ($status -notmatch $serviceName)) {
        return $false
    }

    $probe = docker compose -f $composeFile exec -T $serviceName /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 2>$null
    return $LASTEXITCODE -eq 0
}

while (-not (Test-KafkaReady)) {
    Write-Host "Waiting for Kafka broker to become ready..."
    Start-Sleep -Seconds 5
}

foreach ($topic in $requiredTopics) {
    $createArgs = @(
        "compose", "-f", $composeFile, "exec", "-T", $serviceName,
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server", "localhost:9092",
        "--create",
        "--if-not-exists",
        "--topic", $topic.Name,
        "--partitions", [string]$topic.Partitions,
        "--replication-factor", [string]$topic.ReplicationFactor
    )

    Write-Host "Ensuring topic exists: $($topic.Name)"
    & docker @createArgs

    if ($LASTEXITCODE -ne 0) {
        throw "Failed to create Kafka topic: $($topic.Name)"
    }
}

Write-Host "Final Kafka topic list:"
docker compose -f $composeFile exec -T $serviceName /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
