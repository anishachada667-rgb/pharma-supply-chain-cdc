$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $projectRoot ".env"

if (-not (Test-Path -LiteralPath $envPath)) {
    throw "Create .env from .env.example before registering the connector."
}

$passwordLine = Get-Content -LiteralPath $envPath |
    Where-Object { $_ -match '^SOURCE_DB_PASSWORD=' } |
    Select-Object -First 1

if (-not $passwordLine) {
    throw "SOURCE_DB_PASSWORD is missing from .env."
}

$sourcePassword = ($passwordLine -split '=', 2)[1].Trim()

if ([string]::IsNullOrWhiteSpace($sourcePassword)) {
    throw "SOURCE_DB_PASSWORD is empty."
}

$connectorConfig = @{
    "connector.class" = "io.debezium.connector.postgresql.PostgresConnector"
    "tasks.max" = "1"
    "database.hostname" = "source-db"
    "database.port" = "5432"
    "database.user" = "pharma"
    "database.password" = $sourcePassword
    "database.dbname" = "pharma_source"
    "topic.prefix" = "pharma"
    "plugin.name" = "pgoutput"
    "slot.name" = "pharma_cdc_slot"
    "publication.name" = "pharma_cdc_publication"
    "publication.autocreate.mode" = "filtered"
    "table.include.list" = "operational.suppliers,operational.products,operational.purchase_orders,operational.shipments,operational.inventory"
    "snapshot.mode" = "initial"
    "decimal.handling.mode" = "string"
    "tombstones.on.delete" = "true"
}

$connectorBody = $connectorConfig | ConvertTo-Json

try {
    Invoke-RestMethod `
        -Method Put `
        -Uri "http://127.0.0.1:8083/connectors/pharma-source-connector/config" `
        -ContentType "application/json" `
        -Body $connectorBody | Out-Null

    Write-Output "Connector configuration submitted successfully."

    Invoke-RestMethod `
        -Uri "http://127.0.0.1:8083/connectors/pharma-source-connector/status" |
        ConvertTo-Json -Depth 10
}
finally {
    Remove-Variable sourcePassword, passwordLine, connectorConfig, connectorBody `
        -ErrorAction SilentlyContinue
}