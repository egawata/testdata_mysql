use strict;
use warnings;

use DBI;
use Test::mysqld;
use Test::HandyData::mysql::TableDef;
use Data::Dumper;

use Test::More tests => 2;


main();
exit(0);


sub main {
    my $mysqld = Test::mysqld->new( my_cnf => { 'skip-networking' => '' } )
        or die $Test::mysqld::errstr;

    my $dbh = DBI->connect($mysqld->dsn(dbname => 'test'))
        or die $DBI::errstr;

    test_single_pk($dbh);
    test_multi_pk($dbh);

    $dbh->disconnect();

}

sub test_single_pk {
    my ($dbh) = @_;

    $dbh->do(q{
        CREATE TABLE table_test_0 (
            pid integer primary key,
            test1 varchar(10) not null
        )
    });

    my $td = Test::HandyData::mysql::TableDef->new($dbh, 'table_test_0');
    my $pks = $td->pk_columns();

    is_deeply($pks, [qw/ pid /]);
}


sub test_multi_pk {
    my ($dbh) = @_;

    $dbh->do(q{
        CREATE TABLE table_test_1 (
            pid1 integer not null,
            pid2 integer not null,
            test1 varchar(10) not null,
            primary key (pid1, pid2)
        )
    });

    my $td = Test::HandyData::mysql::TableDef->new($dbh, 'table_test_1');
    my $pks = $td->pk_columns();

    is_deeply($pks, [qw/ pid1 pid2 /]);
}

