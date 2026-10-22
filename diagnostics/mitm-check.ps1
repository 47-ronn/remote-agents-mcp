# =============================================================================
#  mitm-check.ps1 -- is the WIN -> relay path intercepted (MITM / DPI)?
#
#  Checks:
#   1. DNS hijack: local resolver vs DNS-over-HTTPS (Google + Cloudflare)
#   2. Local proxy settings (winhttp + per-user IE/WinINET)
#   3. TLS certificate actually served for relay.claude-code.ink:443
#      - full dump: subject, issuer, serial, validity, SANs, SHA1/SHA256
#      - STRICT system validation (fails if an interceptor presents its own cert)
#      - chain dump even when validation fails (to see WHO intercepts)
#   4. Auto-compare leaf SHA256 against the reference taken from a validated
#      clean path (ojo). Mismatch == interception.
#   5. HTTPS GET /health end-to-end
#   6. tracert to the relay IP (ICMP passes; for the record)
#
#  Run:  powershell -ExecutionPolicy Bypass -File .\mitm-check.ps1
#  Log:  mitm-check-result.txt next to the script.
#  ASCII-only on purpose (PS 5.1 + UTF-8 without BOM is fragile).
# =============================================================================

$RelayHost  = "relay.claude-code.ink"
$RelayIp    = "95.85.237.239"

# Reference leaf-cert SHA256 captured on a validated clean path (ojo, Oct 14):
$ExpectedSha256 = "c1f2a478fe3c8d17908c93a0144392e96932108e341751a212f6971514b9fead"

$Log = Join-Path $PSScriptRoot "mitm-check-result.txt"
Start-Transcript -Path $Log -Force | Out-Null

function Section($t) { Write-Host "`n========== $t ==========" -ForegroundColor Cyan }
function Ok($t)   { Write-Host "  [PASS] $t" -ForegroundColor Green }
function Bad($t)  { Write-Host "  [FAIL] $t" -ForegroundColor Red }
function Warn2($t){ Write-Host "  [WARN] $t" -ForegroundColor Yellow }

# --- 1. DNS hijack check ------------------------------------------------------
Section "1. DNS: local resolver vs DNS-over-HTTPS"
$local = [System.Net.Dns]::GetHostAddresses($RelayHost) | ForEach-Object { $_.ToString() }
Write-Host "  local resolver : $($local -join ', ')"
$doh = @()
foreach ($dohUrl in @("https://dns.google/resolve?name=$RelayHost&type=A",
                      "https://cloudflare-dns.com/dns-query?name=$RelayHost&type=A")) {
    try {
        $r = Invoke-RestMethod -Uri $dohUrl -TimeoutSec 10 -Headers @{ Accept = "application/dns-json" }
        $answers = $r.Answer | Where-Object { $_.type -eq 1 } | ForEach-Object { $_.data }
        Write-Host "  DoH ($($dohUrl.Split('/')[2])): $($answers -join ', ')"
        $doh += $answers
    } catch { Warn2 "DoH unreachable: $($_.Exception.Message)" }
}
if ($doh.Count -gt 0) {
    $match = $false
    foreach ($a in $local) { if ($doh -contains $a) { $match = $true } }
    if ($match) { Ok "local DNS agrees with DoH -- no DNS hijack for this name" }
    else { Bad "local DNS ($($local -join ',')) DIFFERS from DoH ($($doh -join ',')) -- possible hijack!" }
}

# --- 2. Proxy settings ---------------------------------------------------------
Section "2. Local proxy settings"
netsh winhttp show proxy
$ie = Get-ItemProperty "HKCU:\Software\Microsoft\Windows\CurrentVersion\Internet Settings" -ErrorAction SilentlyContinue
Write-Host "  ProxyEnable   : $($ie.ProxyEnable)"
Write-Host "  ProxyServer   : $($ie.ProxyServer)"
Write-Host "  AutoConfigURL : $($ie.AutoConfigURL)"
if ($ie.ProxyEnable -and $ie.ProxyServer) { Warn2 "a system proxy is ACTIVE -- it can intercept TLS" }
else { Ok "no active system proxy" }

# --- 3. TLS certificate as actually served -------------------------------------
Section "3. TLS certificate served for ${RelayHost}:443"
$script:TlsErrors = "n/a"
$tcp = New-Object System.Net.Sockets.TcpClient
$ar = $tcp.BeginConnect($RelayHost, 443, $null, $null)
if (-not $ar.AsyncWaitHandle.WaitOne(15000)) { Bad "TCP connect to :443 timed out"; Stop-Transcript | Out-Null; exit 1 }
$tcp.EndConnect($ar)
$stream = $tcp.GetStream()
$stream.ReadTimeout = 15000
# Bypass validation to CAPTURE whatever is presented (including interceptor certs).
$callback = { param($s, $c, $ch, $e) $script:TlsErrors = [string]$e; return $true }
$ssl = New-Object System.Net.Security.SslStream($stream, $false, $callback)
try { $ssl.AuthenticateAsClient($RelayHost) } catch {
    Bad "TLS handshake failed: $($_.Exception.Message)"
    $tcp.Close(); Stop-Transcript | Out-Null; exit 1
}
$cert = New-Object System.Security.Cryptography.X509Certificates.X509Certificate2($ssl.RemoteCertificate)
$sha256 = -join ([Security.Cryptography.SHA256]::Create().ComputeHash($cert.RawData) | ForEach-Object { $_.ToString("x2") })
$sha1   = -join ([Security.Cryptography.SHA1]::Create().ComputeHash($cert.RawData) | ForEach-Object { $_.ToString("x2") })
Write-Host "  subject    : $($cert.Subject)"
Write-Host "  issuer     : $($cert.Issuer)"
Write-Host "  serial     : $($cert.SerialNumber)"
Write-Host "  valid      : $($cert.NotBefore.ToString('u')) -> $($cert.NotAfter.ToString('u'))"
Write-Host "  SANs       : $($cert.Extensions | Where-Object { $_.Oid.FriendlyName -eq 'Subject Alternative Name' } | ForEach-Object { $_.Format($false) })"
Write-Host "  SHA1       : $sha1"
Write-Host "  SHA256     : $sha256"
Write-Host "  validation : $($script:TlsErrors)"

# Chain (who signed what -- interceptor appliances insert their own CA here)
$chain = New-Object Security.Cryptography.X509Certificates.X509Chain
$chain.ChainPolicy.RevocationMode = "NoCheck"
[void]$chain.Build($cert)
Write-Host "  chain      :"
for ($i = 0; $i -lt $chain.ChainElements.Count; $i++) {
    $el = $chain.ChainElements[$i].Certificate
    Write-Host "    [$i] $($el.Subject)  (by: $($el.Issuer))"
}

# --- 4. Fingerprint comparison --------------------------------------------------
Section "4. Certificate fingerprint vs reference (clean path)"
Write-Host "  expected SHA256 : $ExpectedSha256"
Write-Host "  received SHA256 : $sha256"
if ($sha256 -eq $ExpectedSha256) { Ok "IDENTICAL -- no TLS interception on this path" }
else { Bad "MISMATCH -- the certificate differs from the one served on a clean path = INTERCEPTION" }

# --- 5. Strict validation + end-to-end HTTPS ------------------------------------
Section "5. Strict system TLS validation + GET /health"
try {
    $resp = Invoke-RestMethod -Uri "https://$RelayHost/health" -TimeoutSec 15
    Ok "strict TLS OK; /health = $($resp | ConvertTo-Json -Compress)"
} catch { Bad "strict validation / request failed: $($_.Exception.Message)" }

# --- 6. Route for the record -----------------------------------------------------
Section "6. tracert -d $RelayIp (ICMP passes; for the record)"
tracert -d -h 20 -w 800 $RelayIp

# --- Verdict ----------------------------------------------------------------------
Section "VERDICT"
Write-Host "  - Same SHA256 as clean path  -> no MITM on the 443/TLS path;"
Write-Host "    the dead TCP:4312 is plain filtering (DPI/firewall), not sniffing"
Write-Host "    (passive sniffing cannot blackhole SYNs)."
Write-Host "  - Mismatching SHA256 / alien issuer in chain -> TLS interception:"
Write-Host "    find the appliance (VPN egress 45.151.106.189 or local AV)."

Stop-Transcript | Out-Null
Write-Host "`nLog saved: $Log" -ForegroundColor Cyan
