$WshShell = New-Object -ComObject WScript.Shell

$ProjectDir = (Get-Item -Path ".").FullName
$PythonwExe = "C:\Python314\pythonw.exe"
$ScriptPath = Join-Path $ProjectDir "run_launcher.py"
$IconPath = Join-Path $ProjectDir "vault.ico"
$DesktopDir = [Environment]::GetFolderPath("Desktop")

# 1. Create Shortcut in Project Folder
$ProjectShortcutPath = Join-Path $ProjectDir "Vault.lnk"
$Shortcut1 = $WshShell.CreateShortcut($ProjectShortcutPath)
$Shortcut1.TargetPath = $PythonwExe
$Shortcut1.Arguments = "`"$ScriptPath`""
$Shortcut1.WorkingDirectory = $ProjectDir
$Shortcut1.IconLocation = "$IconPath,0"
$Shortcut1.Description = "Launch Vault Distributed Object Storage System"
$Shortcut1.Save()
Write-Host "[+] Created project shortcut: $ProjectShortcutPath"

# 2. Create Shortcut on Desktop
$DesktopShortcutPath = Join-Path $DesktopDir "Vault.lnk"
$Shortcut2 = $WshShell.CreateShortcut($DesktopShortcutPath)
$Shortcut2.TargetPath = $PythonwExe
$Shortcut2.Arguments = "`"$ScriptPath`""
$Shortcut2.WorkingDirectory = $ProjectDir
$Shortcut2.IconLocation = "$IconPath,0"
$Shortcut2.Description = "Launch Vault Distributed Object Storage System"
$Shortcut2.Save()
Write-Host "[+] Created Desktop shortcut: $DesktopShortcutPath"
