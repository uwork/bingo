# CLAUDE.md — bingo プロジェクトガイド

> **重要**: このプロジェクトでは **日本語で会話してください**。

---

## プロジェクト概要

MySQL binlog をリアルタイムに読み取り、HTTP エンドポイント（主に Fluentd）へ JSON 転送するツール。
go-sql-driver を使わず MySQL プロトコルを独自実装している。

---

## ファイルインデックス

| ファイル | 内容 |
|---|---|
| `main.go` | エントリポイント・CLI フラグ・3つの動作モード |
| `config.go` | Config/MysqlConfig 構造体・設定読み込み |
| `notify.go` | HTTP POST 送信（PostBinary / PostData） |
| `filter/filter.go` | 行フィルタリングロジック |
| `filter/expression.go` | Boolean 式評価エンジン（比較・論理演算子） |
| `filter/expression_test.go` | 式評価テスト |
| `mysql/conn.go` | TCP 接続・ハンドシェイク |
| `mysql/protocol.go` | MySQL プロトコルパーサ |
| `mysql/binlog_dump.go` | COM_BINLOG_DUMP 実装 |
| `mysql/binlog/binlog.go` | BinlogEvent・Column・Row・BinlogParser |
| `mysql/binlog/row_parser.go` | ROW_EVENT パーシング |
| `util/util.go` | Length-Encoded Integer/String・バイト変換 |
| `circle.yml` | CI/CD（タグ `v*.*.*` でクロスコンパイルリリース） |

---

## 開発コマンド

```bash
go build           # ビルド
go test ./...      # テスト
./bingo -genconf   # 設定ファイルサンプル出力
./bingo -c config.json  # 実行
```

---

## コードを読んでも分からない重要知識

### 起動時の binlog 読み取り開始位置

`doStartBinlogRead()` は `show master logs` の**最終行の位置**から読み始める。
起動前の変更イベントは取得できない（過去ログの再生は非対応）。

### バイナリログにはカラム名が含まれない

binlog の仕様上、カラム名は含まれない。転送 JSON の `columns` は名前でなく配列インデックスで扱われる。
フィルタ設定の `$$N` は 0-based のカラムインデックス参照。カラム名でのフィルタは未対応。

### binlog_checksum が有効だと起動時エラー

MySQL 側の設定問題であり、コードのバグではない。
回避策: `mysql -u root -e 'set global binlog_checksum="NONE"'`

### notify.go の既知バグ

`PostBinary` 内の HTTP ステータスログが `200` にハードコードされており、`resp.StatusCode` を参照していない。転送エラーのログが正しく出ない。

### convertVars() の型変換優先順位

`filter/expression.go` の `convertVars()` は Left の型を基準に Right を変換する（Left 優先）。
int → string → Column の順でルールが適用される。変更時は `expression_test.go` で必ず確認すること。

### MySQL プロトコルの独自実装

外部ライブラリ（go-sql-driver 等）を使っていないため、MySQL バージョンアップで互換性が壊れる可能性がある。
`mysql` パッケージを修正する際は外部ドキュメントよりコード内の実装を優先して参照すること。

### DELETE / UPDATE イベントの扱い

イベント自体はパース・フィルタリングされるが未実装扱い。
UPDATE の変更前の値（`Row.BeforeRow`）は転送 JSON に含まれず、DELETE も特別な処理はない。
