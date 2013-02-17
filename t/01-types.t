#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use DBI;

eval "use Test::mysqld";
plan skip_all => "Test::mysqld is needed for test" if $@;

plan tests => 2;


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

    my $hd = Test::HandyData::mysql->new(dbh => $dbh);
    my $desc = $hd->table_def('table_int')->def;

    is(keys %$desc, 1, 'table_int: num of columns');
    is($desc->{val}{DATA_TYPE} , 'int', 'table_int: type of column');
}


__END__

types.t
Check if correct column type names can be retrieved.

