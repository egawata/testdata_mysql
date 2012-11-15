use strict;
use warnings;


use DBI;
use Data::Dumper;
use DateTime;



my $dsn = 'dbi:mysql:dbname=test';
my $user = 'root';

my @VARCHAR_LIST = ( 0..9, 'a'..'z', 'A'..'Z', '_' );
my $COUNT_VARCHAR_LIST = scalar @VARCHAR_LIST;

my $MAX_TINYINT_SIGNED       = 127;
my $MAX_TINYINT_UNSIGNED     = 255;
my $MAX_SMALLINT_SIGNED      = 32767;
my $MAX_SMALLINT_UNSIGNED    = 65535;
my $MAX_INT_SIGNED           = 2147483647;
my $MAX_INT_UNSIGNED         = 4294967295;

my %VALUE_DEF_FUNC = (
    varchar     => \&val_varchar,
    tinyint     => \&val_tinyint,
    smallint    => \&val_smallint,
    int         => \&val_int,
    datetime    => \&val_datetime,
);


main();
exit(0);


sub main {

    my $dbh = DBI->connect($dsn, $user, undef, { AutoCommit => 1, RaiseError => 1 })
        or die $!;

    my $def = get_table_definition($dbh);
    print Dumper($def);

    my @colnames = grep { $def->{$_}{Extra} !~ /auto_increment/ } keys %$def;
    my $cols = join ',', @colnames;
    my $ph   = join ',', ('?') x scalar(@colnames);
    my $sql = "INSERT INTO test ($cols) VALUES ($ph)";

    my $sth = $dbh->prepare($sql);

    for ( 1..100 ) {
        my @values = ();
        for my $key (@colnames) {
            #print $def->{$key}{Type} . "\n";
            my ($type, $size, $opt) = ($def->{$key}{Type} =~ /^(\w+)(?:\((\d+)\))?(\s\w+)?$/);
            my $func = $VALUE_DEF_FUNC{$type}
                or die "Type $type not supported";
            
            my $value = $func->($size, $opt);

            push @values, $value;
        }

        $sth->execute(@values);
    }

    $sth->finish;

    $dbh->disconnect;

}


sub get_table_definition {
    my ($dbh) = @_;

    my $sql = "show full columns from test";
    my $res = $dbh->selectall_hashref($sql, 'Field');

    return $res;
}


sub val_varchar {
    my ($size) = @_;

    my $string = '';
    for (1 .. $size) {
        $string .= $VARCHAR_LIST[ int( rand() * $COUNT_VARCHAR_LIST ) ];
    }

    return $string;
}


sub val_tinyint {
    my ($size, $opt) = @_;

    return (($opt || '') eq 'unsigned') ? int(rand() * $MAX_TINYINT_UNSIGNED) : int(rand() * $MAX_TINYINT_SIGNED);
}

sub val_smallint {
    my ($size, $opt) = @_;

    return (($opt || '') eq 'unsigned') ? int(rand() * $MAX_SMALLINT_UNSIGNED) : int(rand() * $MAX_SMALLINT_SIGNED);
}

sub val_int {
    my ($size, $opt) = @_;

    return (($opt || '') eq 'unsigned') ? int(rand() * $MAX_INT_UNSIGNED) : int(rand() * $MAX_INT_SIGNED);
}


sub val_datetime {
    return DateTime->from_epoch( epoch => rand() * $MAX_INT_UNSIGNED )->datetime();
}


