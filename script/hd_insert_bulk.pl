use strict;
use warnings;

use Test::HandyData::mysql;
use DBI;
use JSON qw(decode_json);
use YAML;
use Getopt::Long;

my %ids = ();
my $req;

main();
exit(0);

sub main {
    my $infile;
    my $debug = 0;
    my ($dbname, $host, $port, $user, $password);
    GetOptions(
        'i|in=s'        => \$infile,
        'd|dbname=s'    => \$dbname,
        'h|host=s'      => \$host,
        'port=i'        => \$port,
        'u|user=s'      => \$user,
        'p|password=s'  => \$password,
        'debug'         => \$debug,
    );
   
    $infile or usage();
    open my $JSON, '<', $infile
        or die "Failed to open infile : $infile : $!";
    my $json = do { local $/; <$JSON> };
    close $JSON;

    my $dsn = "dbi:mysql:dbname=$dbname";
    $host and $dsn .= ";host=$host";
    $port and $dsn .= ";port=$port";

    my $dbh = DBI->connect($dsn, $user, $password, { RaiseError => 1 })
        or die $DBI::errstr;
    $dbh->do("SET NAMES UTF8");
    $dbh->do("begin");

    my $hd = Test::HandyData::mysql->new( dbh => $dbh, fk => 1, debug => $debug );

    eval {
        $req = decode_json($json);

        for my $table (keys %$req) {

            my $list = $req->{$table};
            
            #  データリストが配列だった場合(IDの指定がない場合)は、便宜的にIDを付与する。
            if ( ref $list eq 'ARRAY' ) {
                $list = {};
                my $no = 1;
                for ( @{ $req->{$table} } ) {
                    $list->{$no++} = $_;
                }
            }

            for my $id ( keys %$list ) {
                next if $ids{$table}{$id};
                my $real_id = insert($hd, $table, $list->{$id});
                $ids{$table}{$id} = $real_id;
            }
        }
    };
    if ($@) {
        $dbh->do('rollback');
        die "Failed to insert : $@";
    }
    else {
        $dbh->do('commit');
    }

    print YAML::Dump($hd->inserted);
}




sub insert {
    my ($hd, $table, $rec) = @_;

    my %user_val = ();
    for my $col ( keys %$rec ) {

        if ( $rec->{$col} =~ /^##(\w+)\.(\d+)$/ ) {
            my $ref_table = $1;
            my $ref_id    = $2;
            unless ( $ids{$ref_table}{$ref_id} ) {

                ref($req->{$ref_table}) eq 'HASH'
                    or die "Invalid ID reference. table = $ref_table, ID = $ref_id";

                my $real_id = insert($hd, $ref_table, $req->{$ref_table}{$ref_id});
                $ids{$ref_table}{$ref_id} = $real_id;
            }
            $user_val{$col} = $ids{$ref_table}{$ref_id};
        }
        else {
            $user_val{$col} = $rec->{$col};
        }
    }

    my $id = $hd->insert($table, \%user_val);

    return $id;
}




sub usage {
    print "Usage: $0 -i (json input file)\n";
    exit(-1);
}

