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

=head1 NAME

hd_insert_bulk.pl - Inserts bulk data into mysql, using Test::HandyData.


=head1 VERSION

This documentation refers to hd_insert_bulk.pl 0.0.1

=cut

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



__END__




=head1 USAGE

    $ hd_insert_bulk.pl --infile mysample.json --dbname mydb -u myuser -p mypasswd


=head1 ARGUMENTS

=over 4

=item * -i | --in | --infile

I<< (Required) >> a file in which a list of records to be inserted is written.

=item * -d | --dbname

I<< (Required) >> A name of database

=item * -h | --host

I<< (Optional) >> Hostname of database

=item * --port

I<< (Optional) >> Port no.

=item * -u | --user

I<< (Required) >> User name to connect mysql

=item * -p | --password

I<< (Required) >> Password to connect mysql

=back

 
=head1 DESCRIPTION
 
This application inserts a collection of data into tables. You don't need to specify values to every required fields. You only need to specify values what you're really interested in. If you don't want to consider foreign key constraints, nor the order of insertion (usually you would insert a referenced record at first), it's ok. This application automatically add required fields and records in the right order.

=head2 HOW TO PREPARE INPUT FILE

This application accepts three file formats, I<json>, I<csv> and I<tsv>.

    [Table definitions]

    create table item (
        id integer primary key auto_increment,
        name varchar(20)
    );

    create table customer (
        id integer primary key auto_increment,
        name varchar(50)
    );
        
    create table purchase (
        id integer primary key auto_increment,
        customer_id integer not null,
        item_id integer not null,
        constraint foreign key (customer_id) references customer(id),
        constraint foreign key (item_id) references item(id)
    );



=head3 json

FORMAT:

    {
        (table name) : {
            (id): { (column name): (value), ... },
            ...
        },
        ....
    }

FOR EXAMPLE:

    {
        "item" : {
            "1": { "name": "Apple" },
            "2": { "name": "Banana" }
        },
        "customer": {
            "1": { },
            "2": { }
        },
        "purchase" : {
            "1": { "customer_id" : "##customer.1", "item_id" : "##item.1" },
            "2": { "customer_id" : "##customer.2", "item_id" : "##item.1" },
            "3": { "customer_id" : "##customer.2", "item_id" : "##item.2" }
        }
    }

This will make
    
    [item] (Assuming next auto_increment value is 101)
        +-----+--------+
        | id  | name   |
        |-----+--------|
        | 101 | Apple  |
        | 102 | Banana |
        +-----+--------+

    [customer] (Assuming next auto_increment value is 50)
        +-----+---------+
        | id  | name    |
        |-----+---------|
        |  50 | name_50 |
        |  51 | name_51 |
        +-----+---------+
    
    [purchase] (Assuming next auto_increment value is 501)
        +-----+-------------+---------+
        | id  | customer_id | item_id |
        |-----+-------------+---------+
        | 501 |          50 |     101 |
        | 502 |          51 |     101 |
        | 503 |          51 |     102 |
        +-----+-------------+---------+

NOTE: ID may be omitted if it starts with 1 and is incremented by 1. 

        "purchase" : {
            "1": { "customer_id" : "##customer.1", "item_id" : "##item.1" },
            "2": { "customer_id" : "##customer.2", "item_id" : "##item.1" },
            "3": { "customer_id" : "##customer.2", "item_id" : "##item.2" }
        }

is equivalent to:

        "purchase" : [
            { "customer_id" : "##customer.1", "item_id" : "##item.1" },
            { "customer_id" : "##customer.2", "item_id" : "##item.1" },
            { "customer_id" : "##customer.2", "item_id" : "##item.2" }
        ]

This is especially useful when those records aren't referenced from any other tables and you don't need to care about its ID.


=head3 csv

=head3 tsv


 
=head1 BUGS AND LIMITATIONS

Please report problems to Takashi Egawa (C<< egawa.takashi at gmail com >>)
Patches are welcome.

=head1 AUTHOR

Takashi Egawa  (C<< egawa.takashi at gmail com >>)


=head1 LICENCE AND COPYRIGHT

Copyright (c)2013 Takashi Egawa (C<< egawa.takashi at gmail com >>). All rights reserved.

This module is free software; you can redistribute it and/or
modify it under the same terms as Perl itself. See L<perlartistic>.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. 

