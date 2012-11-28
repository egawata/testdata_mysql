use strict;
use warnings;

use lib qw(lib);

use DBI;
use Test::HandyData::mysql;


main();
exit(0);

sub main {

    my $dbh = DBI->connect('dbi:mysql:test', 'root', undef, { RaiseError => 1 })
        or die $DBI::errstr;

    $dbh->do('DROP TABLE IF EXISTS table1');
    $dbh->do('DROP TABLE IF EXISTS category');
    $dbh->do(<<SQL);      
        create table category (
            id           integer primary key,
            name         varchar(20) not null
        )
SQL
    $dbh->do(<<SQL);
        CREATE TABLE table1 (
            id           integer primary key auto_increment,
            category_id  integer,
            name         varchar(20) not null,
            price        integer not null,
            constraint foreign key (category_id) references category(id)
        )
SQL

    my $handy = Test::HandyData::mysql->new();
    $handy->fk(1);
    $handy->dbh($dbh);
    
    #$handy->insert('table1');
    #$handy->insert('table1', { name => ['Apple', 'Banana', 'Coconut' ], 'category.id' => [ 10, 20, 30 ] }) for 1 .. 10;
    #$handy->insert('table1', { name => ['Apple', 'Banana', 'Coconut' ], 'category_id' => 10 }) for 1 .. 10;
    $handy->insert('table1', { 
                name            => ['Apple', 'Banana', 'Coconut' ], 
                category_id     => [ 10, 20, 30 ], 
                'category.name' => [ 'Vegetable', 'Fruit' ],
    }) for 1 .. 10;
     
    $dbh->disconnect();
       
} 
    


