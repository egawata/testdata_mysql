use strict;
use warnings;

use Test::HandyData::mysql::TableDef;

use Test::More tests => 1;
use Test::mysqld;
use Data::Dumper;


main();
exit(0);


sub main {
    my $mysqld = Test::mysqld->new( my_cnf => { 'skip-networking' => '' } )
        or die $Test::mysqld::errstr;

    my $dbh = DBI->connect($mysqld->dsn(dbname => 'test'))
        or die $DBI::errstr;

    test($dbh);
    
    $dbh->disconnect();
}


sub test {
    my ($dbh) = @_;

    $dbh->do(q{
        CREATE TABLE table_test_0 (
            id integer primary key auto_increment,
            test1 varchar(10) not null
        )
    });

    my $td = Test::HandyData::mysql::TableDef->new($dbh, 'table_test_0');
    my $col_def = $td->column_def('test1');

    isa_ok($col_def, 'Test::HandyData::mysql::ColumnDef');
}

