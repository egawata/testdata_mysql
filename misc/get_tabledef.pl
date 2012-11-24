use strict;
use warnings;


use DBI;
use Data::Dumper;
use DateTime;
use Time::HiRes qw(gettimeofday tv_interval);


my $ONE_YEAR_SEC = 86400 * 365;

my $DBNAME = 'test';

my $DSN = "dbi:mysql:dbname=$DBNAME";
my $USER = 'root';

my $TABLE_NAME = 'test';
my $REF_TABLE_NAME = 'ref_test';

my %cond = ();

my @VARCHAR_LIST = ( 0..9, 'a'..'z', 'A'..'Z', '_' );
my $COUNT_VARCHAR_LIST = scalar @VARCHAR_LIST;

my $MAX_TINYINT_SIGNED       = 127;
my $MAX_TINYINT_UNSIGNED     = 255;
my $MAX_SMALLINT_SIGNED      = 32767;
my $MAX_SMALLINT_UNSIGNED    = 65535;
my $MAX_INT_SIGNED           = 2147483647;
my $MAX_INT_UNSIGNED         = 4294967295;

my %VALUE_DEF_FUNC = (
    char        => \&val_varchar,
    varchar     => \&val_varchar,
    tinyint     => \&val_tinyint,
    smallint    => \&val_smallint,
    int         => \&val_int,
    bigint      => \&val_int,
    float       => \&val_float,
    double      => \&val_float,
    datetime    => \&val_datetime,
    timestamp   => \&val_datetime,
    date        => \&val_datetime,
);


main();
exit(0);


sub main {

    my $dbh = DBI->connect($DSN, $USER, undef, { AutoCommit => 1, RaiseError => 1 })
        or die $!;

    $dbh->do("DROP TABLE IF EXISTS $TABLE_NAME");

    $dbh->do("DROP TABLE IF EXISTS $REF_TABLE_NAME");

    $dbh->do(<<SQL);
        CREATE TABLE $REF_TABLE_NAME (
            id integer primary key auto_increment,
            name varchar(20) not null
        )
SQL

    $dbh->do(<<SQL);
        CREATE TABLE $TABLE_NAME (
            id integer primary key auto_increment,
            name varchar(20) not null,
            price int unsigned not null,
            amount int unsigned not null default 10,
            madein char(2),
            rate double(30,5),
            rate2 float(30,4),
            ref_table_id integer not null,
            created datetime,
            start_date date,
            constraint foreign key (ref_table_id) references $REF_TABLE_NAME (id)
        )
SQL

#    $dbh->do(<<SQL);
#        INSERT INTO $REF_TABLE_NAME (name) values ('testname1'), ('testname2')
#SQL

    process_table($dbh, $REF_TABLE_NAME) for 1..3;
    process_table($dbh, $TABLE_NAME) for 1..100;

    $dbh->disconnect;

}


sub process_table {
    my ($dbh, $table) = @_;

    my $def = get_table_definition($dbh, $table);
    my $constraint = get_constraint($dbh, $table);

    print Dumper($def);

    #  値を指定する必要のある列のみ抽出する
    my @colnames = get_cols_requires_value($def);

    my $cols = join ',', @colnames;
    my $ph   = join ',', ('?') x scalar(@colnames);
    my $sql = "INSERT INTO $table ($cols) VALUES ($ph)";

    my $sth = $dbh->prepare($sql);

    {
        my @values = ();
        for my $key (@colnames) {

            my $type = $def->{$key}{DATA_TYPE};
            my $size = $def->{$key}{CHARACTER_MAXIMUM_LENGTH};
            my $opt  = $def->{$key}{COLUMN_TYPE};

            #  外部キー制約の有無を確認
            my $const_key = $constraint->{$key};
            if ( defined $const_key 
                and $const_key->{REFERENCED_TABLE_SCHEMA} 
                and my $ref_table = $const_key->{REFERENCED_TABLE_NAME} 
                and my $ref_col   = $const_key->{REFERENCED_COLUMN_NAME} ) 
            {

                if ( !defined( $cond{$table}{$key} ) ) {
                    my $ref_res = get_current_ref_keys($dbh, $ref_table, $ref_col); 
                    if ( @$ref_res ) {
                        $cond{$table}{$key}{random} = $ref_res;
                    }
                    else {
                        my $ref_keys = process_table($dbh, $ref_table);
                        $ref_res = get_current_ref_keys($dbh, $ref_table, $ref_col);
                        if ( @$ref_res ) {
                            $cond{$table}{$key}{random} = $ref_res;
                        }
                        else {
                            die "Something is wrong\n";
                        }
                    } 
                }

            }

            my $value;
            if ( my $cond_key = $cond{$table}{$key} ) {

                if ( $cond_key->{random} ) {
                    my $ind = rand() * @{ $cond_key->{random} };
                    $value = $cond_key->{random}[$ind]; 
                }

            } 

            if ( !defined($value) ) {
                my $func = $VALUE_DEF_FUNC{$type}
                    or die "Type $type for $key not supported";
                
                $value = $func->($size, $opt);
            }

            push @values, $value;
        }

        $sth->execute(@values);
    }

    $sth->finish;
}



#  INSERT 実行時に値を指定する必要のある列のみ抽出する
sub get_cols_requires_value {
    my ($def) = @_;

    return grep { 
        $def->{$_}{EXTRA} !~ /auto_increment/           #  auto_increment 列
        and not defined($def->{$_}{COLUMN_DEFAULT})     #  default 値が指定済み
    } keys %$def;
}



sub get_table_definition {
    my ($dbh, $table) = @_;

    my $sql = "SELECT * FROM information_schema.columns WHERE table_schema = ? AND table_name = ?";
    my $res = $dbh->selectall_hashref($sql, 'COLUMN_NAME', undef, $DBNAME, $table);

    return $res;
}


sub get_constraint {
    my ($dbh, $table) = @_;

    my $sql = "SELECT * FROM information_schema.key_column_usage where table_schema = ? AND table_name = ?";
    my $res = $dbh->selectall_hashref($sql, 'COLUMN_NAME', undef, $DBNAME, $table);
    
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

    return (($opt || '') =~ /unsigned/) ? int(rand() * $MAX_TINYINT_UNSIGNED) : int(rand() * $MAX_TINYINT_SIGNED);
}

sub val_smallint {
    my ($size, $opt) = @_;

    return (($opt || '') =~ /unsigned/) ? int(rand() * $MAX_SMALLINT_UNSIGNED) : int(rand() * $MAX_SMALLINT_SIGNED);
}

sub val_int {
    my ($size, $opt) = @_;

    return (($opt || '') =~ /unsigned/) ? int(rand() * $MAX_INT_UNSIGNED) : int(rand() * $MAX_INT_SIGNED);
}


sub val_float {
    my ($size, $opt) = @_;

    return (($opt || '') =~ /unsigned/) ? rand() * $MAX_INT_UNSIGNED : rand() * $MAX_INT_SIGNED;
}


sub val_datetime {
    return DateTime->from_epoch( epoch => time + rand() * 2 * $ONE_YEAR_SEC - $ONE_YEAR_SEC )->datetime();
}


sub get_current_ref_keys {
    my ($dbh, $table, $col) = @_;

    #  現存するレコードを確認
    my $ref_sql = "SELECT DISTINCT $col FROM $table LIMIT 100";
    my $ref_res = $dbh->selectall_arrayref($ref_sql);

    return [ map { $_->[0] } @$ref_res ];
}
