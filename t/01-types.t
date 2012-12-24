#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 2;
use DBI;
use Test::mysqld;

use Test::HandyData::mysql;


main();
exit(0);


sub main {

    my $mysqld = Test::mysqld->new( my_cnf => { 'skip-networking' => '' } )
        or die $Test::mysqld::errstr;

    my $dbh = DBI->connect(
                $mysqld->dsn(dbname => 'test')
    ) or die $DBI::errstr;

    
    $dbh->do("CREATE TABLE table_int (val integer)");

    my $datagen = Test::HandyData::mysql->new(dbh => $dbh);
    my $desc = $datagen->get_table_definition('table_int');

    is(keys %$desc, 1, 'table_int: num of columns');
    is($desc->{val}{DATA_TYPE} , 'int', 'table_int: type of column');
}


