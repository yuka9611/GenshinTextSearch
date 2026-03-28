# Genshin Text Search

## Demo

| 关键词搜索 | 完整对话查看 |
|:---:|:---:|
|![img](.github/demo2.png)|![img](.github/demo1.png)|

## 简介
原神文本搜索工具，可按关键字搜索文本内容，查看来源、完整对话与可读物详情，并在本地已安装对应语音包时播放语音。当前仓库已经演进为 `Flask` 后端 + `Vue 3 / Vite` 前端的结构，适合本地使用、二次开发与打包发布。

可用于：

1. 外语学习
2. 剧情考据
3. 文本整理与检索
4. 本地数据研究或模型训练前的数据定位

## 当前功能

1. 关键词搜索：支持关键词、说话人、语音存在性、创建版本、更新版本组合筛选
2. 任务 / 可读物搜索：支持按名称、版本、任务类别检索，并跳转到详情页
3. 角色语音搜索：支持按角色、标题、版本搜索，并按语音语言播放
4. 角色故事搜索：支持按角色、标题、版本搜索角色故事
5. 设置页：可直接查看已导入文本语言、已检测到的语音包，并通过界面选择游戏资源目录
6. 版本信息：搜索结果与详情页会展示创建版本、更新版本等元数据

## 项目结构

```text
.
├── server/              Flask 服务端、数据库与导入脚本
│   ├── server.py        运行入口
│   ├── config.py        配置读取与运行时路径处理
│   ├── data.db          运行时使用的 SQLite 数据库
│   └── dbBuild/         数据库初始化、全量导入、差量更新脚本
├── webui/               Vue 3 + Vite 前端
└── build_release_mac.sh macOS 打包脚本
```

## 快速开始

### 1. 安装依赖

后端：

```shell
pip install -r server/requirements.txt
```

前端：

```shell
cd webui
npm install
```

### 2. 构建前端

服务端默认托管 `webui/dist`，因此源码运行前需要先构建前端：

```shell
cd webui
npm run build
```

### 3. 启动服务

在仓库根目录执行：

```shell
python server/server.py
```

默认地址：

```text
http://127.0.0.1:5000/
```

程序启动后会尝试自动打开浏览器；如果不希望自动打开，可设置环境变量 `GTS_NO_BROWSER=1`。

## 首次使用

1. 运行时默认读取 `server/data.db`
2. 首次启动如果未配置有效的游戏资源目录，程序会提示选择目录
3. 请选择 `GenshinImpact_Data` / `YuanShen_Data`，或它们上层中可正确定位到 `StreamingAssets` 的目录
4. 进入设置页后，可以继续调整默认搜索语言、结果语言、来源语言和双子显示方式

## 配置文件

配置文件默认位于 `server/config.json`。源码运行时可以直接手动编辑，也可以通过设置页保存。示例：

```json
{
  "resultLanguages": [1, 4],
  "defaultSearchLanguage": 1,
  "assetDir": "D:\\Genshin Impact Game\\YuanShen_Data",
  "sourceLanguage": 1,
  "isMale": "both",
  "enableTextMapFts": true,
  "ftsLangAllowList": [1, 4, 9],
  "ftsTokenizer": "trigram",
  "ftsTokenizerArgs": "",
  "ftsChineseSegmenter": "auto"
}
```

常用字段说明：

1. `resultLanguages`：结果展示语言列表
2. `defaultSearchLanguage`：默认搜索语言
3. `assetDir`：游戏资源目录
4. `sourceLanguage`：来源文本显示语言
5. `isMale`：双子文本显示模式，可为 `false`、`true` 或 `"both"`
6. `enableTextMapFts` / `fts*`：全文检索相关设置

## 数据库导入与更新

旧版 README 中提到的 `server/dbBuild/readme.md` 当前仓库已不存在；现在请直接使用 `server/dbBuild/` 下的脚本。

### 数据源目录

导入脚本默认读取仓库同级的 `AnimeGameData` 目录，也可以通过环境变量 `GTS_DATA_PATH` 指向自己的数据源目录。

### 初始化数据库结构

```shell
cd server/dbBuild
python DBInit.py
```

### 全量导入

```shell
cd server/dbBuild
python DBBuild.py
```

说明：

1. 全量导入过程会按阶段询问是否跳过
2. 导入完成后运行时数据库仍使用 `server/data.db`

### 差量更新

```shell
cd server/dbBuild
python DBBuild.py --diff-update
```

### 仅更新任务相关数据

```shell
cd server/dbBuild
python DBBuild.py --quest-only
```

## 前端开发模式

如果只做前端调试，可以单独启动 Vite 开发服务器，并让它请求本地 Flask：

```shell
cd webui
cp .env.development.example .env.development
npm run dev
```

`.env.development.example` 中默认的后端地址为：

```text
VITE_AXIOS_BASE_URL="http://127.0.0.1:5000/"
```

此时仍需单独启动后端服务。

## 已知问题

1. 目前并非所有文本都做了完整溯源，部分结果仍可能显示为“其他文本”
2. 语音来源类型很多，目前覆盖并不完全
3. 不同语言之间的文本意义并非完全一致，使用时还请自行甄别

## 其他

1. 原神，及其语音和文本版权不属于我，本仓库也不提供语音或文本的下载，提供的数据库仅对文本条目进行了索引，不含具体的文本。
2. 语音直接读取自游戏数据包，并且不会对其进行任何修改
3. pck读取脚本来自于[BUnipendix/WwiseFilePackager](https://github.com/BUnipendix/WwiseFilePackager)
4. wem格式转换使用了[vgmstream](https://github.com/vgmstream/vgmstream)的wasm版本
