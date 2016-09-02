
# What is bingo?

bingo は MySQL のバイナリログをリアルタイム転送するツールです。  
簡単な条件でフィルタリングする事ができます。

# Install

```bash
$ git clone https://github.com/uwork/bingo.git bingo

$ cd bingo

$ go install
```

# Usage

今のところ fluentd 向けに転送する事を想定しています。  
サンプルとして下記の内容で fluentd を起動します。

```bash
$ cat <<EOF > fluentd_sample.conf
<source>
  type http
  port 8888
</source>

<match **>
  type stdout
</match>
EOF
$ fluentd -c fluentd_sample.conf
```


bingo を起動します。  
特に指定しなければパスワード無しのrootユーザで接続します。

```bash
$ bingo
2016/09/02 01:19:10 connected to mysql(root@127.0.0.1:3306)
2016/09/02 01:19:10 start reading binlog
2016/09/02 01:19:10     Binlog Version:  4
2016/09/02 01:19:10     Server Version:  5.7.14-log
```

mysqlでテーブルを作成し、insertを行います。

```bash
$ mysql -u root -e 'create database testdb default character set utf8mb4'
$ mysql -u root -e 'create table testdb.testtable (id bigint, name varchar(32))'
$ mysql -u root -e 'insert into testdb.testtable (id, name) values (1, "hello world")'
$ mysql -u root -e 'insert into testdb.testtable (id, name) values (2, "はろーわーるど")'
```

するとfluentd にデータが届きます。

```bash
2016-09-02 01:23:28 -0400 bingo.data: {"database":"testdb","table":"testtable","columns":["1","hello world"]}
2016-09-02 01:23:36 -0400 bingo.data: {"database":"testdb","table":"testtable","columns":["2","はろーわーるど"]}
```

# Command Options

```bash
Usage of bingo:
  -P int
        mysql server port (default 3306)
  -c string
        config file path
  -d string
        destinate for binlog data. (default "http://localhost:8888/bingo.data")
  -genconf
        generate config.
  -h string
        mysql server ip address (default "127.0.0.1")
  -p string
        mysql password
  -u string
        mysql user (default "root")
  -v    show version
```

# Config

設定ファイルのサンプルは以下の通りです。

```bash
$ bingo -genconf
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
        "columns": [ 0, 1, 2 ],
        "where": {
          "left": "$$0", "op": "=", "right": "1"
        }
      }
    ]
  }
}
```

* filter の where にバイナリログをマッチさせる条件を記述します。
* 上記のサンプルをsql的に記述すると、"select カラム0, カラム1, カラム2 from dbname.tablename where カラム0 = '1'" となります。
* op には = != &gt; &gt;= &lt; &lt;= が使用可能です。
* バイナリログにはカラム名が含まれない為、カラム番号での指定を行います。  
(desc table等を併用して列名での指定を可能にする案はあります)

複合条件を設定したい場合、下記の様に条件式オブジェクトをネストして記述します。

```bash
    "filters": [
      {
        "database": "dbname",
        "table": "tablename",
        "columns": [ 0, 1, 2 ],
        "where": {
          "left": {"left": "$$0", "op": "=", "right": "1"},
          "op": "and",
          "right": {"left": "$$2", "op": "=", "right": "10"}
        }
      }
```

# Issue

* 全般的にテストが書けていない
* カラム名でフィルタを設定できない
* delete,update時の挙動が未実装
* 設定のリロード
* goroutine 等を使用して全体的なパフォーマンスチューニング
* 巨大なinsert等を行った場合の挙動が未実装
* LOAD DATAに未対応
* 古いバージョンのMySQL(&lt;=5.6)のバイナリログに未対応
* libmysqlclient や go-sql-driver/mysql を使わず、自前で実装しているため、MySQLアップデートに脆弱

# Environment

* Go 1.5
* MySQL 5.7.14
* MySQL Binlog Version V4

# License

MITライセンスに準拠します。


# Deduction

本ソフトウェアによって起こったいかなる事象についても制作者は責任を負いません。  
すべて自己責任にてご利用ください。


