# Genshin Text Search

原神文本搜索工具，支持本地检索文本、任务、可读物、角色语音、角色故事、NPC 对话与图鉴条目，并在本地已安装对应语音包时直接播放语音。项目当前采用 `Flask` 后端 + `Vue 3 / Vite` 前端，适合本地使用与二次开发。

## 主要功能

1. 关键词搜索：支持关键词、说话人、语音存在性、来源类型、创建版本、更新版本组合筛选。
2. 名称搜索：支持任务与可读物名称检索，并可按任务类别、说话人、可读物分类和版本过滤。
3. NPC 对话搜索：支持按 NPC 名称和版本筛选，并查看其关联对白。
4. 角色语音搜索：支持按角色、标题、版本搜索语音条目，并按语音语言播放。
5. 角色故事搜索：支持按角色、标题、版本搜索故事文本。
6. 图鉴搜索：支持按主分类、子分类、版本和关键词搜索实体条目，并查看关联文本。
7. 设置页：可查看已导入文本语言、已检测到的语音包，选择游戏资源目录，并调整默认搜索语言、结果语言、来源语言和旅行者显示模式。

## 项目结构

```text
.
├── server/              Flask 服务端、SQLite 数据库、导入与增量更新脚本
│   ├── server.py        运行入口
│   ├── config.py        配置读取、运行时目录与路径处理
│   ├── data.db          运行时数据库路径
│   └── dbBuild/         数据库初始化、全量导入、差量更新脚本
├── tests/               pytest 测试
└── webui/               Vue 3 + Vite 前端
```

## 环境要求

1. `Python 3`
2. `Node.js` 与 `npm`
3. 本地原神游戏资源目录
4. 可选：`tkinter`，用于首次启动时弹出资源目录选择框

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

1. 运行时数据库路径固定为 `server/data.db`。
2. 如果没有配置有效的游戏资源目录，程序会在可用时弹出本地目录选择框。
3. 请选择 `GenshinImpact_Data`、`YuanShen_Data`，或其上层中可正确定位到 `StreamingAssets` / `Persistent` 的目录。
4. 进入设置页后，可以继续调整默认搜索语言、结果语言、来源语言和旅行者显示模式。

## 配置文件

配置文件优先写入 `server/config.json`；如果 `server/` 不可写，则会回退到：

```text
~/.genshin_text_search/config.json
```

示例：

```json
{
  "resultLanguages": [1, 4, 9],
  "defaultSearchLanguage": 1,
  "assetDir": "D:\\Genshin Impact Game\\YuanShen_Data",
  "sourceLanguage": 1,
  "isMale": "both",
  "enableTextMapFts": true,
  "ftsLangAllowList": [1, 4, 9],
  "ftsTokenizer": "trigram",
  "ftsTokenizerArgs": "",
  "ftsChineseSegmenter": "auto",
  "ftsJiebaUserDict": "",
  "ftsExtensionPath": "",
  "ftsExtensionEntry": "",
  "ftsStopwords": [],
  "ftsMinTokenLength": 1,
  "ftsMaxTokenLength": 32
}
```

常用字段说明：

1. `resultLanguages`：结果展示语言列表。
2. `defaultSearchLanguage`：默认搜索语言。
3. `assetDir`：游戏资源目录。
4. `sourceLanguage`：来源文本显示语言。
5. `isMale`：旅行者文本显示模式，兼容旧配置中的布尔值与 `"both"`。
6. `enableTextMapFts`：是否启用 TextMap 全文检索。
7. `ftsTokenizer`：SQLite FTS tokenizer 名称，默认 `trigram`。
8. `ftsChineseSegmenter`：中文分词模式，可选 `auto`、`jieba`、`char_bigram`、`none`。
9. `ftsExtensionPath` / `ftsExtensionEntry`：自定义 FTS 扩展入口。
10. `ftsStopwords`、`ftsMinTokenLength`、`ftsMaxTokenLength`：查询期 token 过滤配置。

## 数据库导入与更新

导入脚本位于 `server/dbBuild/`。默认读取仓库同级的 `AnimeGameData` 目录，也可以通过环境变量 `GTS_DATA_PATH` 指向自己的数据源目录。

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

1. 全量导入过程会按阶段询问是否跳过。
2. 导入完成后，运行时仍使用 `server/data.db`。

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

## 开发说明

### 前端开发模式

如果只做前端调试，可以单独启动 Vite 开发服务器，并让它请求本地 Flask：

```shell
cd webui
cp .env.development.example .env.development
npm run dev
```

`.env.development.example` 默认后端地址：

```text
VITE_AXIOS_BASE_URL="http://127.0.0.1:5000/"
```

此时仍需单独启动后端服务。

### 运行测试

仓库包含 `config.py`、API、控制器与 FTS 分词逻辑的 pytest 测试：

```shell
pytest
```

## 已知限制

1. 目前并非所有文本都做了完整溯源，部分结果仍可能显示为“其他文本”。
2. 语音来源类型很多，目前覆盖并不完全。
3. 不同语言之间的文本意义并非完全一致，使用时还请自行甄别。
4. 如果运行环境没有可用的 `tkinter` 或是无界面环境，资源目录选择框可能不可用，此时请通过设置页或配置文件手动填写路径。

## 说明

1. 原神及其语音、文本版权不属于本仓库作者，本仓库不提供游戏文本或语音资源下载。
2. 语音直接读取自本地游戏数据包，不会对原始文件进行修改。
3. pck 读取脚本来自 [BUnipendix/WwiseFilePackager](https://github.com/BUnipendix/WwiseFilePackager)。
4. wem 格式转换使用了 [vgmstream](https://github.com/vgmstream/vgmstream) 的 wasm 版本。
