#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Exception;
use DBI;
use Test::mysqld;
use Data::Dumper;

use Test::HandyData::mysql;


main();
exit(0);


#  determine_fk_value

sub main {
    
    my $mysqld = Test::mysqld->new( my_cnf => { 'skip-networking' => '' } )
        or plan skip_all => $Test::mysqld::errstr;

    my $dbh = DBI->connect(
                $mysqld->dsn(dbname => 'test')
    ) or die $DBI::errstr;
    $dbh->{RaiseError} = 1;
    my $hd = Test::HandyData::mysql->new(dbh => $dbh);

    test_with_valspec($dbh, $hd);
    test_without_valspec($dbh, $hd);
    test_without_valspec_2($dbh, $hd);

    $dbh->disconnect();

    done_testing();
}


#  Test with valspec
sub test_with_valspec {
    my ($dbh, $hd) = @_;

    $dbh->do(q{
        CREATE TABLE table1 (
            id integer primary key
        )
    });
    $dbh->do(q{
        CREATE TABLE table2 (
            id integer primary key,
            table1_id integer,
            constraint foreign key (table1_id) references table1(id)
        )
    });
    $dbh->do(q{INSERT INTO table1 (id) VALUES (100)});

    #  値の決定方法に指定があり、かつ参照先レコードがすでに存在する場合
    #    -> ID は指定に従う。参照先テーブルにレコードは追加しない。
    $hd->_set_user_valspec('table2', { table1_id => 100 });
    is($hd->determine_fk_value('table2', 'table1_id', { table => 'table1', column => 'id' }), 100);
    is(rowcount($dbh, 'table1'), 1);
    select_all($dbh, 'table1');
    select_all($dbh, 'table2');

    #  値の決定方法に指定があり、かつ参照先レコードが存在しない場合
    #    -> ID は指定に従う。さらにそのIDを持つレコードを参照先テーブルに追加する。
    $hd->_set_user_valspec('table2', { table1_id => 200 });
    is($hd->determine_fk_value('table2', 'table1_id', { table => 'table1', column => 'id' }), 200);
    is(rowcount($dbh, 'table1'), 2);
    my @res = $dbh->selectrow_array(qq{
        SELECT COUNT(*) FROM table1 WHERE id = 200
    });
    is($res[0], 1); 
    select_all($dbh, 'table1');
    select_all($dbh, 'table2');

}


#  Test without valspec
sub test_without_valspec {
    my ($dbh, $hd) = @_;

    $dbh->do(q{
        CREATE TABLE table11 (
            id integer primary key
        )
    });
    $dbh->do(q{
        CREATE TABLE table12 (
            id integer primary key,
            table11_id integer,
            constraint foreign key (table11_id) references table11(id)
        )
    });

    #  値の決定方法に指定がなく、かつ参照先テーブルにレコードが1件も存在しない場合
    #    -> ID を適当に決めて参照先テーブルにレコード追加。
    #       そのIDを参照元の値とする。
    $hd->_set_user_valspec('table12', {});
    my $refid = $hd->determine_fk_value('table12', 'table11_id', { table => 'table11', column => 'id' });
    is(rowcount($dbh, 'table11'), 1);    #  レコードが追加された
    
    my @res = $dbh->selectrow_array(qq{
        SELECT COUNT(*) FROM table11 WHERE id = ?  
    }, undef, $refid);
    is($res[0], 1); 
    select_all($dbh, 'table11');
    select_all($dbh, 'table12');

}


#  Test without valspec
sub test_without_valspec_2 {
    my ($dbh, $hd) = @_;

    $dbh->do(q{
        CREATE TABLE table21 (
            id integer primary key
        )
    });
    $dbh->do(q{
        CREATE TABLE table22 (
            id integer primary key,
            table21_id integer,
            constraint foreign key (table21_id) references table21(id)
        )
    });
    $dbh->do(q{INSERT INTO table21 (id) VALUES (100)});
    $dbh->do(q{INSERT INTO table21 (id) VALUES (200)});


    #  値の決定方法に指定がなく、かつ参照先テーブルにレコードが数件存在する場合
    #    -> 参照先テーブルにあるレコードの中から1件を選び、
    #       そのIDを参照元の値とする。
    $hd->_set_user_valspec('table22', {});
    my $refid = $hd->determine_fk_value('table22', 'table21_id', { table => 'table21', column => 'id' });
    is(rowcount($dbh, 'table21'), 2);    #  レコードは追加されない
    ok($refid == 100 or $refid == 200);
    
    select_all($dbh, 'table21');
    select_all($dbh, 'table22');

}


sub rowcount {
    my ($dbh, $table) = @_;

    my @res = $dbh->selectrow_array(qq{
        SELECT COUNT(*) FROM $table
    });

    return $res[0];
}


sub select_all {
    my ($dbh, $table) = @_;

    return;
   
   
    #  Below is for debug. 
    my $res = $dbh->selectall_arrayref(qq{
        SELECT * FROM $table
    });

    print $table . ':' . Dumper($res);

}   
