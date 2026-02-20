# CLAUDE.md — bingo プロジェクトガイド

> **重要**: このプロジェクトでは **日本語で会話してください**。コメント・コミットメッセージ・説明はすべて日本語で行うことを推奨します。

---

## プロジェクト概要

**bingo** は MySQL のバイナリログ（binlog）をリアルタイムに読み取り、外部の HTTP エンドポイント（主に Fluentd）へ JSON 形式で転送するツールです。

- バイナリログの変更イベントを監視し、条件に一致した行データを転送する
- Fluentd の `type http` 入力プラグインを主なターゲットとしている
- MySQL との通信は `go-sql-driver` を使わず、**独自の MySQL プロトコル実装**で行う

---

## 対応環境

| 項目 | バージョン |
|---|---|
| Go | 1.5 以上 |
| MySQL | 5.7.14 以上（binlog V4 のみ対応） |
| Binlog バージョン | V4 |

> MySQL の `binlog_checksum` が有効な場合はエラーになります。
> 事前に `set global binlog_checksum="NONE"` を実行してください。

---

## ディレクトリ構成

```
bingo/
├── main.go              # エントリポイント。CLIオプション定義・モード振り分け
├── config.go            # Config/MysqlConfig 構造体・LoadConfig/DumpConfig
├── notify.go            # HTTP POST 送信関数（PostBinary / PostData）
├── main_test.go         # mainパッケージのテスト（現在ほぼ空）
├── circle.yml           # CircleCI CI/CD 設定
├── filter/
│   ├── filter.go        # FilterConfig・FilteredRow・行フィルタリングロジック
│   ├── expression.go    # Boolean式の評価エンジン（比較・論理演算子）
│   └── expression_test.go # 式評価のユニットテスト
├── mysql/
│   ├── conn.go          # MySQL TCP接続・ハンドシェイク
│   ├── command.go       # MySQL コマンド定義
│   ├── protocol.go      # MySQL プロトコルパーサ
│   ├── binlog_dump.go   # COM_BINLOG_DUMP コマンド実装
│   ├── protocol_test.go # プロトコルパーサのテスト
│   └── binlog/
│       ├── binlog.go       # BinlogEvent・Column・Row・BinlogParser
│       ├── row_parser.go   # ROW_EVENT のパーシング
│       ├── row_parser_test.go
│       └── util.go         # binlog 内部ユーティリティ
└── util/
    └── util.go          # 汎用バイト変換ユーティリティ
```

---

## パッケージ責務

### `main` パッケージ
- CLI フラグの定義と解析（`flag` 標準ライブラリ使用）
- 3つの動作モード:
  - `doStartBinlogRead()` — binlog の読み取りと転送（通常起動）
  - `doDumpConfig()` — 設定ファイルのサンプル出力（`-genconf`）
  - `doVersion()` — バージョン表示（`-v`）
- `PostBinary` / `PostData` で転送先 URL へ HTTP POST

### `config` パッケージ（`main` 内）
- `Config` 構造体: MySQL 接続情報 + 転送先 URL + フィルタ設定
- JSON ファイルから設定を読み込む（`-c` オプション）
- CLI オプションをデフォルト値として使用し、設定ファイルで上書き

### `filter` パッケージ
- `FilterConfig.FilterEvent()` — BinlogEvent を受け取り、条件に一致する行を JSON バイト列で返す
- フィルタ未設定時は全行を転送
- `database` / `table` でスキーマ・テーブルを絞り込み
- `columns` で出力するカラムのインデックスを指定（未指定時は全カラム）
- `where` で行単位の条件評価

### `filter.Expression` — 式評価エンジン
- `Left`, `Op`, `Right` の3フィールドで構成される再帰的な式ツリー
- サポートする演算子:

| 定数 | 記号 | 説明 |
|---|---|---|
| `OP_EQ` | `=` | 等値比較 |
| `OP_NE` | `!=` | 不等値比較 |
| `OP_LE` | `<=` | 以下 |
| `OP_LT` | `<` | 未満 |
| `OP_GE` | `>=` | 以上 |
| `OP_GT` | `>` | 超過 |
| `OP_OR` | `or` | 論理和 |
| `OP_AND` | `and` | 論理積 |

- カラム参照構文: `$$N`（N は 0-based のカラムインデックス）
- 型変換: `int` ↔ `string` ↔ `binlog.Column` の自動変換あり

### `mysql` パッケージ
- `Open()` — TCP 接続 + MySQL ハンドシェイク（Keep-Alive 有効）
- `conn.DumpBinlog()` — `COM_BINLOG_DUMP` で最新 binlog からストリーム読み取り
- **独自プロトコル実装のため MySQL バージョンアップへの追従が必要**

### `mysql/binlog` パッケージ
- `BinlogParser.ParseBinlogEvent()` — バイナリデータを BinlogEvent にパース
- 対応イベントタイプ:
  - `FORMAT_DESCRIPTION_EVENT` (0x0f)
  - `QUERY_EVENT` (0x02)
  - `TABLE_MAP_EVENT` (0x13)
  - `WRITE_ROWS_EVENT` v1/v2 (0x17, 0x1e)
  - `UPDATE_ROWS_EVENT` v1/v2 (0x18, 0x1f)
  - `DELETE_ROWS_EVENT` v1/v2 (0x19, 0x20)
- `Column` 型: 全 MySQL データ型に対応し `.Int()` `.String()` `.Double()` `.Bytes()` `.Time()` で変換可能

### `util` パッケージ
- MySQL プロトコルの Length-Encoded Integer / String のパーシング
- リトルエンディアンのバイト ↔ `uint32` / `int` 変換

---

## CLI オプション

```
-u string    MySQLユーザー名 (デフォルト: "root")
-p string    MySQLパスワード (デフォルト: 空文字)
-h string    MySQLホスト (デフォルト: "127.0.0.1")
-P int       MySQLポート (デフォルト: 3306)
-d string    転送先URL (デフォルト: "http://localhost:8888/bingo.data")
-c string    設定ファイルパス
-genconf     設定ファイルのサンプルを標準出力して終了
-v           バージョン表示して終了
```

---

## 設定ファイル形式（JSON）

```json
{
  "mysql": {
    "user": "root",
    "pass": "",
    "host": "127.0.0.1",
    "port": 3306
  },
  "dest": "http://localhost:8888/bingo.data",
  "filter": {
    "filters": [
      {
        "database": "dbname",
        "table": "tablename",
        "columns": [0, 1, 2],
        "where": {
          "left": "$$0",
          "op": "=",
          "right": "1"
        }
      }
    ]
  }
}
```

### 複合条件の例（AND / OR）

```json
"where": {
  "left": { "left": "$$0", "op": "=", "right": "1" },
  "op": "and",
  "right": { "left": "$$2", "op": ">=", "right": "10" }
}
```

---

## 転送データ形式

binlog から取得した行は以下の JSON 配列として HTTP POST されます:

```json
[
  {
    "database": "testdb",
    "table": "testtable",
    "columns": ["1", "hello world"]
  }
]
```

- カラムはすべて文字列に変換される
- バイナリログにはカラム名が含まれないため、**カラム名は出力されない**

---

## 開発ワークフロー

### ビルド

```bash
go build
# または
go install
```

### テスト実行

```bash
go test ./...
```

> 現状テストカバレッジは低いです。`expression_test.go` と `row_parser_test.go` に実装例があります。

### 設定ファイル生成

```bash
./bingo -genconf > config.json
```

### 実行

```bash
./bingo -c config.json
```

---

## CI/CD（CircleCI）

- タイムゾーン: `Asia/Tokyo`
- タグ `v*.*.*` がプッシュされると自動リリース:
  1. `gox` でクロスコンパイル（linux/amd64, linux/arm, darwin/amd64, windows/amd64）
  2. 各バイナリを zip 圧縮（`distpkg/bingo_<OS>_<Arch>.zip`）
  3. `ghr` で GitHub Releases へアップロード

必要な環境変数:
- `GITHUB_TOKEN` — GitHub API トークン
- `CIRCLE_USERNAME`, `CIRCLE_PROJECT_REPONAME`, `CIRCLE_TAG` — CircleCI 自動設定

---

## 既知の制限事項・未実装項目

| 項目 | 状態 |
|---|---|
| テストの充実 | 未対応（テストがほぼない） |
| カラム名でのフィルタリング | 未対応（インデックスのみ） |
| DELETE / UPDATE イベントの処理 | 未実装 |
| 設定のホットリロード | 未実装 |
| goroutine によるパフォーマンス改善 | 未実装 |
| 巨大な INSERT への対応 | 未実装 |
| `LOAD DATA` への対応 | 未対応 |
| MySQL 5.6 以前（binlog V3）のサポート | 未対応 |
| binlog_checksum が有効な場合のエラー | 回避策: `set global binlog_checksum="NONE"` |

---

## コーディング規約

- Go の標準フォーマット（`gofmt`）に従う
- パッケージ名はディレクトリ名と一致させる
- エラーハンドリング: 致命的なエラーは `log.Fatal`、軽微なエラーは `log.Println`
- JSON タグはすべてのエクスポートフィールドに付与する
- バイナリログのカラムインデックスは常に 0-based

---

## AI アシスタントへの注意事項

- **会話は日本語で行うこと**
- MySQL プロトコルは独自実装のため、外部ライブラリのドキュメントではなくコード内の実装を参照すること
- `filter/expression.go` の `convertVars()` は型変換の優先順位が重要。変更時は `expression_test.go` で動作確認すること
- `binlog_checksum` 関連のエラーは MySQL 側の設定問題であり、コードの問題ではない
- 現在 binlog の最新位置から読み始める実装になっており、過去ログの再生には非対応
- `notify.go` の `PostBinary` は HTTP ステータスの判定ログが `200` ハードコードになっているバグがある（`resp.StatusCode` を参照すべき）
