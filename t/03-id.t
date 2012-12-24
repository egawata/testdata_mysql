#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 2;
use DBI;
use Test::mysqld;

use Test::HandyData::mysql;


main();
exit(0);


=pod

ID 列(primary key)のテスト

ID は以下のケースを想定する。

(0)insert() の呼び出し元メソッドで明示的に指定
その値をそのまま使う。

(1)単一列、整数型、auto_increment あり。
auto_increment の値に従う

(2)単一列、整数型、auto_increment なし。
既存の最大値 + 1 とする。
初回のみ、最大値を取得するクエリを発行する。その値を保持しておき、次回以降はその値をインクリメントしながら使用する。

(3)単一列、文字列型
必ずuniqueとなる2文字の文字列を生成し prefix とする。キー全体は (prefix)_(通し番号)のようにする。
ただし文字数が足りない場合は、(prefix)+(文字) (後ろの文字は、利用可能な文字を順番に使用していく)
(後日実装)


(4)複合key(型は問わない)
すべてをランダムに生成する。
(後日実装)


=cut

sub main {

    my $mysqld = Test::mysqld->new( my_cnf => { 'skip-networking' => '' } )
        or die $Test::mysqld::errstr;

    my $dbh = DBI->connect(
                $mysqld->dsn(dbname => 'test')
    ) or die $DBI::errstr;

    test_0($dbh); 
    #test_1($dbh);
     

    $dbh->disconnect;
}


=pod test_0

以下のケースをテストする

(0)insert() の呼び出し元メソッドで明示的に指定
その値をそのまま使う。

=cut

sub test_0 {
    my ($dbh) = @_;

    $dbh->do(q{
        CREATE TABLE table_test_0 (
            id      integer primary key auto_increment,
            str     varchar(10)
        )
    });
    $dbh->do(q{ALTER TABLE table_test_0 AUTO_INCREMENT = 100});  #  next ID = 100

    my $hd = Test::HandyData::mysql->new(dbh => $dbh);

    my $id = $hd->insert('table_test_0', { id => 99 });
    is($id, 99);

    $id = $hd->insert('table_test_0');
    is($id, 100);
}


=pod test_1

以下のケースをテストする

(1)単一列、整数型、auto_increment あり。
auto_increment の値に従う

=cut

sub test_1 {

}




