Write-Host ""
Write-Host "=== MEDIA CAMPAIGN MANAGEMENT - CONTROL PANEL ==="
Write-Host ""

# Detect project path
$projectPath = Split-Path -Parent $MyInvocation.MyCommand.Path
$composeFile = Join-Path $projectPath "docker-compose.yml"

if (!(Test-Path $composeFile)) {
    Write-Host "ERROR: docker-compose.yml not found at: $composeFile"
    exit 1
}

function Start-Services {
    Write-Host ""
    Write-Host "=== STARTING FULL STACK ==="

    Write-Host "Stopping existing containers..."
    docker-compose -f $composeFile down --remove-orphans

    Write-Host "Building Docker images..."
    docker-compose -f $composeFile build

    Write-Host "Starting containers..."
    docker-compose -f $composeFile up -d

    Write-Host "Waiting for containers to initialize..."
    Start-Sleep -Seconds 10

    Write-Host ""
    Write-Host "=== Docker Containers Status ==="
    docker ps

    Write-Host ""
    Write-Host "Kafka Producer now runs INSIDE DOCKER."
    Write-Host ""

    Write-Host "Running Analytics Builder..."
    # Ensure the Python environment in the host/container can run this. If this should run inside a container,
    # replace this with docker exec into the appropriate container.
    python -m src.analytics.build_analytics

    Write-Host ""
    Write-Host "=== SERVICES READY ==="
    Write-Host "FastAPI Docs:         http://localhost:8000/docs"
    Write-Host "Prometheus:           http://localhost:9090"
    Write-Host "Grafana:              http://localhost:3000"
    Write-Host "Consumer Metrics:     http://localhost:8001/metrics"
    Write-Host "Kafka Broker:         kafka:9092"
    Write-Host "Grafana Login:        admin / admin"
}

function Stop-Services {
    Write-Host ""
    Write-Host "=== STOPPING ALL DOCKER SERVICES ==="
    docker-compose -f $composeFile down
    Write-Host "All services stopped."
}

function Restart-Services {
    Write-Host ""
    Write-Host "=== RESTARTING STACK ==="
    Stop-Services
    Start-Services
}

while ($true) {
    Write-Host ""
    Write-Host "Choose an action:"
    Write-Host "1. Start All Services"
    Write-Host "2. Stop All Services"
    Write-Host "3. Restart Services"
    Write-Host "4. Exit"
    Write-Host ""
    $choice = Read-Host "Enter option (1-4)"

    switch ($choice) {
        "1" { Start-Services }
        "2" { Stop-Services }
        "3" { Restart-Services }
        "4" {
            Write-Host "Exiting..."
            break
        }
        default {
            Write-Host "Invalid choice. Please select 1-4."
        }
    }
}
