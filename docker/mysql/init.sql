-- bingo が binlog を読むためのレプリケーションユーザ作成
CREATE USER IF NOT EXISTS 'bingo'@'%' IDENTIFIED BY 'bingo';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'bingo'@'%';
FLUSH PRIVILEGES;

-- サンプルデータベース・テーブル（動作確認用）
CREATE DATABASE IF NOT EXISTS testdb DEFAULT CHARACTER SET utf8mb4;
CREATE TABLE IF NOT EXISTS testdb.testtable (
    id   BIGINT,
    name VARCHAR(32)
);
