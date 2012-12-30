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

get_cols_requiring_value のテスト


=cut

sub main {

    my $mysqld = Test::mysqld->new( my_cnf => { 'skip-networking' => '' } )
        or die $Test::mysqld::errstr;

    my $dbh = DBI->connect(
                $mysqld->dsn(dbname => 'test')
    ) or die $DBI::errstr;

    test_0($dbh); 

    $dbh->disconnect;
}


=pod test_0

auto_increment 列は結果から除外される。
ただしユーザから指定があれば結果に含まれる。


=cut

sub test_0 {
    my ($dbh) = @_;

    $dbh->do(q{
        CREATE TABLE table_test_0 (
            id      integer primary key auto_increment
        )
    });

    my $hd = Test::HandyData::mysql->new(dbh => $dbh);

    my $cols = $hd->get_cols_requiring_value('table_test_0');
    is_deeply($cols, []);

    $hd->set_user_cond('table_test_0', { id => 100 });
    $cols = $hd->get_cols_requiring_value('table_test_0');
    is_deeply($cols, ['id']);
}

