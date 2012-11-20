#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 2;
use DBI;
use Test::mysqld;

use Test::DataGen::mysql;


main();
exit(0);


sub main {

    my $mysqld = Test::mysqld->new( my_cnf => { 'skip-networking' => '' } )
        or die $Test::mysqld::errstr;

    my $dbh = DBI->connect(
                $mysqld->dsn(dbname => 'test')
    ) or die $DBI::errstr;

    
    $dbh->do("CREATE TABLE table_int (val integer)");

    my $datagen = Test::DataGen::mysql->new($dbh);
    my $desc = $datagen->desc('table_int');

    is(keys %$desc, 1, 'table_int: num of columns');
    is($desc->type('val'), 'int', 'table_int: type of column');
}


