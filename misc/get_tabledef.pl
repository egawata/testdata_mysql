use v5.14;

use DBI;
use Data::Dumper;
use DateTime;



my $dsn = 'dbi:mysql:dbname=test';
my $user = 'root';

my @varchar_list = ( 0..9, 'a'..'z', 'A'..'Z', '_' );
my $count_varchar_list = scalar @varchar_list;

my $MAXINT_SIGNED = 2147483647;
my $MAXINT_UNSIGNED = 4294967295;


main();
exit(0);


sub main {

    my $dbh = DBI->connect($dsn, $user, undef, { AutoCommit => 1, RaiseError => 1 })
        or die $!;

    my $def = get_table_definition($dbh);
    print Dumper($def);


    my @colnames = grep { $res->{$_}{Extra} !~ /auto_increment/ } keys %$res;
    my $cols = join ',', @colnames;
    my $ph   = join ',', ('?') x scalar(@colnames);
    $sql = "INSERT INTO test ($cols) VALUES ($ph)";

    my $sth = $dbh->prepare($sql);

    for ( 1..100 ) {
        my @values = ();
        for my $key (@colnames) {
            my $value;

            my $def = $res->{$key};

            my $type = $def->{Type};
            if ($type =~ /^varchar\((\d+)\)$/) {
                my $string = '';
                for (1 .. $1) {
                    $string .= $varchar_list[ int( rand() * $count_varchar_list ) ];
                }
                $value = $string;

            }
            elsif ( $type =~ /^int\(/ ) {
                $value = int(rand() * $MAXINT_SIGNED);

            }
            elsif ( $type eq 'datetime' ) {
                $value = DateTime->from_epoch( epoch => rand() * $MAXINT_UNSIGNED )->datetime();

            }

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



__END__
$VAR1 = { 
          'create_time' => { 
                             'Extra' => '',
                             'Default' => undef,
                             'Comment' => '',
                             'Field' => 'create_time',
                             'Type' => 'datetime',
                             'Privileges' => 'select,insert,update,references',
                             'Null' => 'YES',
                             'Key' => '',
                             'Collation' => undef
                           },
          'name' => { 
                      'Extra' => '',
                      'Default' => undef,
                      'Comment' => '',
                      'Field' => 'name',
                      'Type' => 'varchar(100)',
                      'Privileges' => 'select,insert,update,references',
                      'Null' => 'YES',
                      'Key' => '',
                      'Collation' => 'latin1_swedish_ci'
                    },
          'price' => { 
                       'Extra' => '',
                       'Default' => undef,
                       'Comment' => '',
                       'Field' => 'price',
                       'Type' => 'int(11)',
                       'Privileges' => 'select,insert,update,references',
                       'Null' => 'YES',
                       'Key' => '',
                       'Collation' => undef
                     },
          'id' => { 
                    'Extra' => 'auto_increment',
                    'Default' => undef,
                    'Comment' => '',
                    'Field' => 'id',
                    'Type' => 'int(11)',
                    'Privileges' => 'select,insert,update,references',
                    'Null' => 'NO',
                    'Key' => 'PRI',
                    'Collation' => undef
                  }
        };

