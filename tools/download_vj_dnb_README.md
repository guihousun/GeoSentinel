# VJ102DNB / VJ103DNB 下载说明

本工具用于把 LAADS 查询 JSON 中的链接批量下载到本地。

- 脚本：`tools/download_vj_dnb.py`
- 输入：`tools/query_vj_dnb_laads_json.py` 生成的 JSON（或同结构 JSON）
- 输出：`.nc` 文件 + `download_manifest.json`

## 1. 前置条件

1. 已安装 `curl`（Windows 建议可用 `curl.exe --version` 自检）。
2. 已配置 Earthdata token（推荐放在项目根目录 `.env`）：

```env
EARTHDATA_TOKEN=你的token
```

脚本会自动加载：
- `E:\地缘环境智能计算平台\.env`
- 当前工作目录 `.env`

## 2. 命令

```powershell
conda run -n 地缘环境智能计算平台 python tools/download_vj_dnb.py `
  --input "E:\Download\LAADS_query.test_vj102_vj103.json" `
  --output "E:\地缘环境智能计算平台\base_data\VJ102DNB_Iran"
```

可选参数：

- `--token-env`：token 环境变量名，默认 `EARTHDATA_TOKEN`。

## 3. 成功判定

脚本会对下载结果做格式校验（HDF5/netCDF 头）：

- 成功：打印 `success: xx.xx MB`
- 失败：打印 `failed: ...` 并在结尾返回非 0 退出码

最终统计写入：

- `输出目录/download_manifest.json`

## 4. 常见问题

1. `warning: EARTHDATA_TOKEN is empty`
- 说明未读取到 token，请检查 `.env` 文件位置或变量名。

2. `curl rc=35` / 证书吊销错误
- 脚本已在 Windows 自动加 `--ssl-no-revoke`，若仍失败，通常是网络链路问题（代理/VPN/防火墙）。

3. `received HTML page instead of data file`
- 说明拿到的是登录页/错误页，不是真实数据文件。优先检查 token 是否有效。

