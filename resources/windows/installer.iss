#define MyAppName "DBFlux"
#ifndef MyAppVersion
#define MyAppVersion "0.3.0"
#endif
#define MyAppPublisher "Ignacio Perez"
#define MyAppURL "https://github.com/0xErwin1/dbflux"
#define MyAppExeName "dbflux.exe"

[Setup]
AppId={{D8A3E981-6A8D-4B8F-9E09-86D8DEB7A6F1}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={autopf}\{#MyAppName}
DisableProgramGroupPage=yes
LicenseFile=LICENSE-MIT
OutputDir=output
OutputBaseFilename=dbflux-windows-amd64-setup
SetupIconFile=dbflux.ico
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
MinVersion=10.0
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
UninstallDisplayIcon={app}\{#MyAppExeName}
UninstallDisplayName={#MyAppName}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked

[Files]
Source: "dbflux.exe"; DestDir: "{app}"; Flags: ignoreversion
Source: "dbflux.ico"; DestDir: "{app}"; Flags: ignoreversion
Source: "LICENSE-MIT"; DestDir: "{app}"; Flags: ignoreversion
Source: "LICENSE-APACHE"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; IconFilename: "{app}\dbflux.ico"
Name: "{group}\{cm:ProgramOnTheWeb,{#MyAppName}}"; Filename: "{#MyAppURL}"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; IconFilename: "{app}\dbflux.ico"; Tasks: desktopicon

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[Registry]
Root: HKCU; Subkey: "Software\{#MyAppName}"; ValueType: string; ValueName: "InstallPath"; ValueData: "{app}"
Root: HKCU; Subkey: "Software\{#MyAppName}"; ValueType: string; ValueName: "Version"; ValueData: "{#MyAppVersion}"
