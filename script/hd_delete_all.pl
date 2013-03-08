#!/usr/bin/perl

use strict;
use warnings;

use DBI;
use YAML qw(LoadFile);
use Test::HandyData::mysql::TableDef;

main();
exit(0);


sub main {
    my $infile = $ARGV[0] or usage();

    my $dbh = DBI->connect()
        or die $DBI::errstr;
    $dbh->{RaiseError} = 1;

    my $inserted = LoadFile($infile);

    if ( ref $inserted eq 'HASH' ) {

        $dbh->do(q{SET FOREIGN_KEY_CHECKS = 0});
        for my $table ( keys %$inserted ) {
            my $ids = $inserted->{$table};
            my $table_def = Test::HandyData::mysql::TableDef->new($dbh, $table);
            my $pk_column = $table_def->pk_columns()->[0];
            
            my $sth = $dbh->prepare(qq{DELETE FROM $table WHERE $pk_column = ?});
            for my $id (@$ids) {
                my $numrow = $sth->execute($id);
                if ( $numrow >= 1 ) {
                    print "Deleted $table ($id)\n";
                }
                else {
                    print "No row affected. $table ($id)\n";
                }
            }
        }
        $dbh->do(q{SET FOREIGN_KEY_CHECKS = 1});

    }
    else {
        die "Invalid format : $infile\n";

    }

    $dbh->disconnect();

}

sub usage {
    print "Usage: $0 (filename.yml)\n";
    exit(-1); 
}



