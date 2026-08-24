# NTL-GPT Windows Server 操作指南

本文档用于维护当前 NTL-GPT Windows Server 部署，适合日常启动、停止、同步代码和故障排查。

## 1. 当前部署信息

| 项目 | 当前配置 |
|---|---|
| 系统 | Windows Server 2016 Standard |
| 项目目录 | `D:\NTL-GPT\NTL-GPT-main` |
| Conda 环境 | `NTL-GPT-Stable` |
| Streamlit 地址 | `127.0.0.1:8501` |
| 公网域名 | `https://ntl-gpt.gischaser.cn` |
| Nginx 目录 | `C:\nginx-1.29.8` |
| Nginx 配置 | `C:\nginx-1.29.8\conf\nginx.conf` |
| HTTPS 证书 | `C:\nginx-1.29.8\cert` |
| win-acme | `C:\win-acme` |
| PostgreSQL | PostgreSQL 18，默认端口 `5432` |
| PostgreSQL 数据库 | `ntl_langgraph` |
| PostgreSQL 用户 | `ntl` |

不要把 `.env`、数据库密码、API Key、GEE 凭据或 Earthdata Token 提交到 Git。

## 2. 服务器重启后的启动步骤

### 2.1 启动 Nginx

以管理员身份打开 PowerShell：

```powershell
Get-Process nginx -ErrorAction SilentlyContinue
```

如果没有输出，启动 Nginx：

```powershell
Start-Process `
  -FilePath "C:\nginx-1.29.8\nginx.exe" `
  -WorkingDirectory "C:\nginx-1.29.8" `
  -WindowStyle Hidden
```

检查配置和进程：

```powershell
cd C:\nginx-1.29.8
.\nginx.exe -t
Get-Process nginx
```

### 2.2 检查 PostgreSQL

```powershell
Test-NetConnection 127.0.0.1 -Port 5432
```

`TcpTestSucceeded` 应为 `True`。需要进一步检查时执行：

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" `
  -h 127.0.0.1 -p 5432 -U ntl -d ntl_langgraph `
  -c "select version();"
```

命令会要求输入 PostgreSQL 用户 `ntl` 的密码。

### 2.3 启动 Streamlit

打开 Miniconda PowerShell Prompt：

```powershell
conda activate NTL-GPT-Stable
cd D:\NTL-GPT\NTL-GPT-main
python check_env.py
streamlit run Streamlit.py --server.address 127.0.0.1 --server.port 8501
```

`python check_env.py` 应显示：

```text
Result: READY
```

保持这个窗口开启。关闭窗口或按 `Ctrl+C` 会停止 Streamlit。

### 2.4 检查网站

另开一个 PowerShell 窗口：

```powershell
Invoke-WebRequest http://127.0.0.1:8501/_stcore/health -UseBasicParsing
Invoke-WebRequest https://ntl-gpt.gischaser.cn/_stcore/health -UseBasicParsing
```

两个请求都应返回 HTTP `200`，正文通常是 `ok`。

浏览器访问：

<https://ntl-gpt.gischaser.cn>

## 3. 从 GitHub 同步最新版本

先在运行 Streamlit 的窗口按 `Ctrl+C`，避免更新过程中仍有旧代码运行。

```powershell
conda activate NTL-GPT-Stable
cd D:\NTL-GPT\NTL-GPT-main

git status
git pull --ff-only origin main
git log -1 --oneline
```

如果 `git status` 显示服务器上有手动修改，不要使用 `git reset --hard`。先检查具体文件并保留截图或备份，再决定如何处理。

### 3.1 更新运行环境

普通代码更新通常不需要重新创建 Conda 环境。以下情况建议更新环境：

- `environment.yml` 有变化；
- 拉取了大量新工具或依赖；
- 启动时报 `ModuleNotFoundError`。

执行：

```powershell
conda env update -n NTL-GPT-Stable -f environment.yml
conda activate NTL-GPT-Stable
pip install -e .\packages\ntl_toolkit
```

然后检查并重新启动：

```powershell
python check_env.py
streamlit run Streamlit.py --server.address 127.0.0.1 --server.port 8501
```

Git 不会覆盖被 `.gitignore` 忽略的 `.env`，但新增环境变量需要手动补充到服务器的 `.env`。

## 4. 日常停止和重启

### 停止 Streamlit

在 Streamlit 窗口按：

```text
Ctrl+C
```

### 查找占用 8501 端口的进程

```powershell
Get-NetTCPConnection -LocalPort 8501 -State Listen -ErrorAction SilentlyContinue
```

查看具体命令：

```powershell
Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match "streamlit.*Streamlit.py" } |
  Select-Object ProcessId, Name, CommandLine
```

只有在确认它是 NTL-GPT 的 Streamlit 进程后，才能按进程号停止：

```powershell
Stop-Process -Id <ProcessId>
```

### 重载 Nginx

修改 `nginx.conf` 或证书更新后执行：

```powershell
cd C:\nginx-1.29.8
.\nginx.exe -t
.\nginx.exe -s reload
```

只有在 `nginx.exe -t` 显示配置测试成功后才能 reload。

### 停止 Nginx

```powershell
cd C:\nginx-1.29.8
.\nginx.exe -s quit
```

## 5. `.env` 检查

服务器 `.env` 位于：

```text
D:\NTL-GPT\NTL-GPT-main\.env
```

当前前端模型至少需要：

```env
DeepSeek_API_KEY=
DeepSeek_Coding_URL=
DASHSCOPE_API_KEY=
DASHSCOPE_Qwen_plus_KEY=
DASHSCOPE_Qwen_plus_URL=
```

生产数据库需要：

```env
NTL_HISTORY_DB_URL=postgresql://...
NTL_LANGGRAPH_POSTGRES_URL=postgresql://...
```

其他常用配置包括：

```env
GEE_DEFAULT_PROJECT_ID=
EARTHDATA_TOKEN=
amap_api_key=
```

不要在聊天、截图或 Git 提交中显示变量值。配置后使用以下命令验证是否完整：

```powershell
python check_env.py
```

## 6. Nginx 和 HTTPS

### 检查 Nginx 配置

```powershell
cd C:\nginx-1.29.8
.\nginx.exe -t
```

### 检查证书文件

```powershell
Get-ChildItem C:\nginx-1.29.8\cert
```

通常应包含域名对应的 `chain.pem` 和 `key.pem` 文件。

### 检查 win-acme 自动续期任务

```powershell
Get-ScheduledTask |
  Where-Object { $_.TaskName -match "win-acme" } |
  Select-Object TaskName, State
```

手动触发续期检查：

```powershell
cd C:\win-acme
.\wacs.exe --renew --baseuri "https://acme-v02.api.letsencrypt.org/"
```

续期成功后重载 Nginx：

```powershell
cd C:\nginx-1.29.8
.\nginx.exe -t
.\nginx.exe -s reload
```

建议每月检查一次自动续期任务和证书到期时间。

## 7. 常见故障

### 激活智能体时报 `ASN1: NOT_ENOUGH_DATA`

这表示当前 Python/OpenSSL 无法解析 Windows 系统证书库，并不表示 Nginx 的 HTTPS 证书失效。新版 NTL-GPT 会在仅检测到这一特定错误时自动改用 `certifi` 证书包，并继续执行正常的 HTTPS 证书校验。

更新项目后先执行：

```powershell
conda activate NTL-GPT-Stable
Set-Location D:\NTL-GPT\NTL-GPT-main
python check_env.py
```

看到下面任意一项都可以启动：

- `Outbound SSL` 下显示 `SYSTEM_DEFAULT: OK`
- `Outbound SSL` 下显示 `CERTIFI_FALLBACK: OK`

不要为解决该错误批量删除 Windows 的 `CA` 或 `ROOT` 证书。

### 网站显示 502 Bad Gateway

先检查 Streamlit：

```powershell
Invoke-WebRequest http://127.0.0.1:8501/_stcore/health -UseBasicParsing
```

- 本地请求失败：Streamlit 没有启动、启动失败或端口被占用。
- 本地成功但公网返回 502：检查 Nginx 配置、进程和反向代理地址。

```powershell
cd C:\nginx-1.29.8
.\nginx.exe -t
Get-Content .\logs\error.log -Tail 100
```

### `streamlit` 无法识别

```powershell
conda activate NTL-GPT-Stable
python -m streamlit run Streamlit.py --server.address 127.0.0.1 --server.port 8501
```

仍然失败时检查：

```powershell
where.exe python
python -c "import sys; print(sys.executable)"
python -m pip show streamlit
```

### `ModuleNotFoundError`

```powershell
conda env update -n NTL-GPT-Stable -f environment.yml
conda activate NTL-GPT-Stable
pip install -e .\packages\ntl_toolkit
python check_env.py
```

### 数据库连接失败

```powershell
Test-NetConnection 127.0.0.1 -Port 5432
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" `
  -h 127.0.0.1 -p 5432 -U ntl -d ntl_langgraph `
  -c "select current_database(), current_user;"
```

同时检查 `.env` 中的数据库名、用户、密码、主机和端口。

### `git pull` 连接 GitHub 失败

先确认网络：

```powershell
Test-NetConnection github.com -Port 443
git remote -v
```

不要反复删除项目或重新克隆。网络恢复后再次执行：

```powershell
git pull --ff-only origin main
```

### Git 提示本地修改冲突

```powershell
git status --short
git diff
```

不要直接运行 `git reset --hard` 或删除文件。先确认这些修改是否需要保留。

## 8. 备份

### PostgreSQL 数据库

```powershell
$date = Get-Date -Format "yyyyMMdd-HHmmss"
New-Item -ItemType Directory -Force D:\NTL-GPT\backups | Out-Null
& "C:\Program Files\PostgreSQL\18\bin\pg_dump.exe" `
  -h 127.0.0.1 -p 5432 -U ntl -d ntl_langgraph `
  -Fc -f "D:\NTL-GPT\backups\ntl_langgraph-$date.dump"
```

### 查找实际工作空间目录

```powershell
conda activate NTL-GPT-Stable
cd D:\NTL-GPT\NTL-GPT-main
python -c "from storage_manager import storage_manager; print('user_data:', storage_manager.base_dir); print('base_data:', storage_manager.shared_dir)"
```

根据命令输出备份用户上传、输出结果和线程记忆。`.env` 也应单独保存在受控位置，但不能上传到公开仓库。

## 9. 最短操作清单

### 每次服务器重启后

```powershell
# 管理员 PowerShell：确认或启动 Nginx
Get-Process nginx -ErrorAction SilentlyContinue

# Miniconda PowerShell Prompt：启动 NTL-GPT
conda activate NTL-GPT-Stable
cd D:\NTL-GPT\NTL-GPT-main
python check_env.py
streamlit run Streamlit.py --server.address 127.0.0.1 --server.port 8501
```

### 每次同步 GitHub 后

```powershell
cd D:\NTL-GPT\NTL-GPT-main
git pull --ff-only origin main
git log -1 --oneline
python check_env.py
streamlit run Streamlit.py --server.address 127.0.0.1 --server.port 8501
```

### 网站打不开时

```powershell
Test-NetConnection 127.0.0.1 -Port 5432
Invoke-WebRequest http://127.0.0.1:8501/_stcore/health -UseBasicParsing
Invoke-WebRequest https://ntl-gpt.gischaser.cn/_stcore/health -UseBasicParsing
cd C:\nginx-1.29.8
.\nginx.exe -t
Get-Content .\logs\error.log -Tail 100
```
