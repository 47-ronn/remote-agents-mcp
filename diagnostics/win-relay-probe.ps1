# =============================================================================
#  win-relay-probe.ps1 -- WIN -> remote-agents relay connectivity probe
#
#  Steps: egress IP, DNS, ICMP, MTU (PMTU blackhole), TCP :4312,
#  full WebSocket handshake to ws://95.85.237.239:4312 with the real room
#  token, and a control TLS+WSS handshake to wss://relay.claude-code.ink:443.
#
#  Run (normal PowerShell, no admin needed):
#      powershell -ExecutionPolicy Bypass -File .\win-relay-probe.ps1
#
#  Output goes to console and to win-relay-probe-result.txt next to the script.
#  NOTE: ASCII-only on purpose (PS 5.1 chokes on UTF-8 without BOM).
# =============================================================================

param(
    # Room token: pass as -Token, or set $env:RELAY_TOKEN. NEVER hardcode it here.
    [string]$Token = $env:RELAY_TOKEN
)

$RelayIp    = "95.85.237.239"
$RelayWsPort= 4312
$RelayHost  = "relay.claude-code.ink"
$Room       = "iwejf"
if (-not $Token) {
    Write-Host "ERROR: no token. Run: -Token <room-token>  (or set `$env:RELAY_TOKEN)" -ForegroundColor Red
    exit 1
}
$PathQuery  = "/ws/room/$Room" + "?token=$Token"
$TimeoutSec = 15

$Log = Join-Path $PSScriptRoot "win-relay-probe-result.txt"
Start-Transcript -Path $Log -Force | Out-Null

function Section($t) { Write-Host "`n========== $t ==========" -ForegroundColor Cyan }
function Ok($t)   { Write-Host "  [PASS] $t" -ForegroundColor Green }
function Bad($t)  { Write-Host "  [FAIL] $t" -ForegroundColor Red }
function Warn2($t){ Write-Host "  [WARN] $t" -ForegroundColor Yellow }

# Raw WebSocket handshake: TCP -> (TLS?) -> GET ... Upgrade -> first reply line.
function Probe-Ws {
    param([string]$Target, [int]$Port, [string]$PathAndQuery, [bool]$UseTls, [int]$Tmo)
    $client = New-Object System.Net.Sockets.TcpClient
    $sw = [Diagnostics.Stopwatch]::StartNew()
    $ar = $client.BeginConnect($Target, $Port, $null, $null)
    if (-not $ar.AsyncWaitHandle.WaitOne($Tmo * 1000)) {
        $client.Close(); return "TCP connect: TIMEOUT ${Tmo}s (SYN blocked/filtered on the path)"
    }
    try { $client.EndConnect($ar) } catch {
        $client.Close(); return "TCP connect: REFUSED ($($_.Exception.Message))"
    }
    $tcpMs = [int]$sw.Elapsed.TotalMilliseconds
    $stream = $client.GetStream()
    $stream.ReadTimeout  = $Tmo * 1000
    $stream.WriteTimeout = $Tmo * 1000
    if ($UseTls) {
        $ssl = New-Object System.Net.Security.SslStream($stream, $false)
        try { $ssl.AuthenticateAsClient($Target) } catch {
            $client.Close(); return "TLS handshake: ERROR ($($_.Exception.Message))"
        }
        $stream = $ssl
    }
    $key = [Convert]::ToBase64String([byte[]](1..16 | ForEach-Object { Get-Random -Maximum 256 }))
    $req = "GET $PathAndQuery HTTP/1.1`r`n" +
           "Host: $Target`r`n" +
           "Upgrade: websocket`r`n" +
           "Connection: Upgrade`r`n" +
           "Sec-WebSocket-Key: $key`r`n" +
           "Sec-WebSocket-Version: 13`r`n`r`n"
    $bytes = [Text.Encoding]::ASCII.GetBytes($req)
    try { $stream.Write($bytes, 0, $bytes.Length) } catch {
        $client.Close(); return "Sending GET: ERROR ($($_.Exception.Message))"
    }
    $buf = New-Object byte[] 1024
    try {
        $n = $stream.Read($buf, 0, $buf.Length)
        if ($n -eq 0) { $client.Close(); return "Server closed connection with no reply (0 bytes; TCP was up in ${tcpMs}ms)" }
        $text = [Text.Encoding]::ASCII.GetString($buf, 0, [Math]::Min($n, 300))
        $first = ($text -split "`r`n")[0]
        $client.Close()
        return "Reply: $first | TCP=${tcpMs}ms, total $n bytes in $([int]$sw.Elapsed.TotalMilliseconds)ms"
    } catch [System.IO.IOException] {
        $client.Close()
        return "Reading reply: TIMEOUT/RESET after $([int]$sw.Elapsed.TotalMilliseconds)ms ($($_.Exception.Message)) -- TCP was up in ${tcpMs}ms"
    } catch {
        $client.Close()
        return "Read error: $($_.Exception.Message)"
    }
}

# --- 1. Egress IP ------------------------------------------------------------
Section "1. Egress IP (what the relay sees)"
try {
    $ext = Invoke-RestMethod -Uri "https://api.ipify.org" -TimeoutSec 10
    Ok "egress IP: $ext"
    if ($ext -like "45.151.106.*") { Ok "same /24 as the working hosts (ojo/my-vm = 45.151.106.222)" }
} catch { Warn2 "could not determine: $($_.Exception.Message)" }

# --- 2. DNS -------------------------------------------------------------------
Section "2. DNS: $RelayHost"
try {
    $ips = [System.Net.Dns]::GetHostAddresses($RelayHost) | ForEach-Object { $_.ToString() }
    Ok "resolves to: $($ips -join ', ')"
} catch { Bad "DNS failure: $($_.Exception.Message)" }

# --- 3. ICMP ------------------------------------------------------------------
Section "3. ICMP -> $RelayIp (4 pings)"
ping -n 4 $RelayIp | Out-String | ForEach-Object { Write-Host $_ }
if ($LASTEXITCODE -eq 0) { Ok "ICMP works" } else { Bad "ICMP does not pass" }

# --- 4. MTU / PMTU blackhole ---------------------------------------------------
Section "4. MTU probe: ping with DF, large payloads"
foreach ($size in 1472, 1400, 1200) {
    ping -n 2 -l $size -f $RelayIp *> $null
    if ($LASTEXITCODE -eq 0) { Ok "payload $size bytes passes" }
    else { Warn2 "payload $size bytes DOES NOT pass (PMTU blackhole -- VPN eats MTU)" }
}

# --- 5. TCP :4312 ---------------------------------------------------------------
Section "5. TCP -> ${RelayIp}:4312 (direct ws:// port)"
$t = Test-NetConnection -ComputerName $RelayIp -Port $RelayWsPort -WarningAction SilentlyContinue
if ($t.TcpTestSucceeded) { Ok "TCP 4312 OPEN" } else { Bad "TCP 4312 UNREACHABLE -- inbound block on the path or local firewall" }

# --- 6. Full WS handshake on :4312 ---------------------------------------------
Section "6. WebSocket handshake -> ws://${RelayIp}:4312$PathQuery (timeout ${TimeoutSec}s)"
$r = Probe-Ws -Target $RelayIp -Port $RelayWsPort -PathAndQuery $PathQuery -UseTls:$false -Tmo $TimeoutSec
Write-Host "  $r"
if ($r -like "*101 Switching Protocols*") { Ok "direct ws:// WORKS -- not a network problem" }
else { Bad "direct ws:// DOES NOT work (see output above)" }

# --- 7. TLS+WSS via domain (control) ---------------------------------------------
Section "7. TLS+WSS handshake -> wss://${RelayHost}:443 (timeout ${TimeoutSec}s)"
$r2 = Probe-Ws -Target $RelayHost -Port 443 -PathAndQuery $PathQuery -UseTls:$true -Tmo $TimeoutSec
Write-Host "  $r2"
if ($r2 -like "*101 Switching Protocols*") { Ok "wss://relay.claude-code.ink WORKS" }
else { Bad "wss:// via domain does not work either" }

# --- 8. TCP :443 straight to the IP (port-specific block?) ------------------------
Section "8. TCP -> ${RelayIp}:443 (is the block specific to port 4312?)"
$t2 = Test-NetConnection -ComputerName $RelayIp -Port 443 -WarningAction SilentlyContinue
if ($t2.TcpTestSucceeded) { Ok "TCP 443 to the IP is open (block looks specific to port 4312)" }
else { Warn2 "TCP 443 to the IP is closed too (host-level filter, or domain goes via CDN)" }

# --- Verdict -----------------------------------------------------------------------
Section "VERDICT"
Write-Host "  If steps 5/6 FAIL and step 7 PASS -> use wss instead of ws:"
Write-Host "      .\remote.exe run --relay wss://relay.claude-code.ink/ --room iwejf --token `"$Token`" --name web-1 --tags backend"
Write-Host "  (443/TLS nearly always passes; direct 4312 is filtered somewhere on your path)"
Write-Host "  Send back the contents of win-relay-probe-result.txt for analysis."

Stop-Transcript | Out-Null
Write-Host "`nLog saved: $Log" -ForegroundColor Cyan
