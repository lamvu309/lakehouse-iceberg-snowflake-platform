# Download Iceberg + AWS JARs required for PySpark
# Usage: powershell -File scripts/download_jars.ps1

$jarsDir = Join-Path $PSScriptRoot "..\jars"
New-Item -ItemType Directory -Force -Path $jarsDir | Out-Null

$jars = @(
    @{
        Name = "iceberg-spark-runtime.jar"
        Url  = "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
    },
    @{
        Name = "hadoop-aws.jar"
        Url  = "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
    },
    @{
        Name = "aws-java-sdk-bundle.jar"
        Url  = "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
    }
)

foreach ($jar in $jars) {
    $dest = Join-Path $jarsDir $jar.Name
    if (Test-Path $dest) {
        Write-Host "⏭️  Already exists: $($jar.Name)"
    } else {
        Write-Host "⬇️  Downloading $($jar.Name) ..."
        Invoke-WebRequest -Uri $jar.Url -OutFile $dest -UseBasicParsing
        $sizeMB = [math]::Round((Get-Item $dest).Length / 1MB, 1)
        Write-Host "✅ $($jar.Name) ($sizeMB MB)"
    }
}

Write-Host ""
Write-Host "✅ All JARs ready in ./jars/"
Get-ChildItem $jarsDir | Select-Object Name, @{N='Size(MB)';E={[math]::Round($_.Length/1MB,1)}}
