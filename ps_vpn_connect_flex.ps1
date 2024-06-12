$vpnName = "Azure Flex VPN";
$vpn = Get-VpnConnection -Name $vpnName;
if($vpn.ConnectionStatus -eq "Disconnected"){
    rasdial $vpnName;
}