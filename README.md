# Tag Process

这是一个用于处理标签（Tag）数据的 Python 工具，旨在清洗、过滤并整合标签别名信息。

## 功能

1.  **加载数据**：读取 `tags.parquet`（标签数据）和 `tag_alias.csv`（别名数据）。
2.  **数据清洗**：
    -   删除不需要的列（如 `created_at`, `updated_at`, `index`, `words`）。
    -   过滤掉 `post_count` 小于 20 的标签。
3.  **排序**：按 `post_count` 降序排列。
4.  **别名整合**：将 `tag_alias.csv` 中的别名信息解析并合并到主标签数据中，生成 `alias` 列（包含别名列表）。
5.  **输出**：生成处理后的文件 `tags_processed.parquet`.

## 环境要求

本项目使用 `uv` 进行依赖管理和运行。

-   Python >= 3.13
-   uv

## 安装与运行

1.  **安装依赖**：
    ```bash
    uv sync
    ```

2.  **运行脚本**：
    ```bash
    uv run main.py
    ```

3.  **结果**：
    运行完成后，会在当前目录下生成 `tags_processed.parquet` 文件。

## 文件说明

-   `main.py`: 主处理脚本。
-   `pyproject.toml`: 项目配置及依赖。
-   `tags.parquet`: 原始标签数据（输入）。
-   `tag_alias.csv`: 原始别名数据（输入）。
-   `tags_processed.parquet`: 处理后的数据（输出）。
