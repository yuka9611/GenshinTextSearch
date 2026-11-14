# Cloudflare Pages 自动发布

前端仓库为 `yuka9611/GenshinTextSearch`，生产分支为 `master`，正式站为
https://genshin-text-search.pages.dev/。

现有 Pages 项目使用 Direct Upload，通过
`.github/workflows/deploy-pages.yml` 接入 GitHub Actions。推送 `webui/`
或工作流本身的改动后，Actions 执行 Node 测试和生产构建，再发布到现有项目；
也可在 GitHub Actions 页面手动运行该工作流。

这条流水线只发布前端和 Pages Functions。VM 上的 Python API、数据库及
AstrBot 插件按各自部署流程更新。

## 仓库配置

在 GitHub 仓库的 Settings → Secrets and variables → Actions 中配置：

| 类型 | 名称 | 内容 |
| --- | --- | --- |
| Secret | `CLOUDFLARE_API_TOKEN` | 当前 Cloudflare 账号的 Cloudflare Pages Edit 专用 API Token |
| Variable | `CLOUDFLARE_ACCOUNT_ID` | Pages 所在账号 ID |
| Variable | `VITE_SUPABASE_URL` | 生产 Supabase 公开 URL |
| Variable | `VITE_SUPABASE_PUBLISHABLE_KEY` | 生产 Supabase 公开浏览器 key |
| Variable | `VITE_AUTH_PROVIDERS` | 已配置的登录提供方，默认 `github,google` |

CI 使用专用 API Token，不使用个人 Wrangler OAuth 会话。不要将 Cloudflare
令牌或 Supabase secret/service-role key 写进代码、日志或文档。

## API 绑定

`webui/wrangler.toml` 维护生产配置：Pages Function 的 `database` Service Binding
指向 `genshin-text-search-api` 的 `production` 环境。浏览器 API 请求保持同源，
因此 `VITE_AXIOS_BASE_URL` 留空。

修改 Wrangler 配置会影响生产绑定；发布后应确认 `/healthz` 返回
`status: ok` 和 `cloudMode: true`。工作流自动执行这项检查。

## 验证与回滚

本地验证可将构建输出放在临时目录，保留现有 `webui/dist`：

```bash
node --test webui/tests/*.test.mjs
cd webui && npm run build -- --outDir /tmp/gts-pages-build
```

发布前应配置与生产一致的公开 Supabase 构建变量。当前已发布代码可在
GitHub Actions 运行记录中按 commit SHA 追踪，Cloudflare 部署也携带同一 SHA。

需要回滚时，在 Cloudflare Pages 的生产部署历史中恢复已验证的旧部署。
如果源分支仍有待发布的新改动，先暂停工作流，避免后续推送再次覆盖回滚版本。

参考：[Cloudflare CI 部署指南](https://developers.cloudflare.com/pages/how-to/use-direct-upload-with-continuous-integration/)
及 [Pages Wrangler 配置](https://developers.cloudflare.com/pages/functions/wrangler-configuration/)。
