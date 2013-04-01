#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::Exception;
use DBI;
use Test::mysqld;
use Data::Dumper;

use Test::HandyData::mysql;


main();
exit(0);


#   process_table 


sub main {
    
    my $mysqld = Test::mysqld->new( my_cnf => { 'skip-networking' => '' } )
        or plan skip_all => $Test::mysqld::errstr;

    my $dbh = DBI->connect(
                $mysqld->dsn(dbname => 'test')
    ) or die $DBI::errstr;
    $dbh->{RaiseError} = 1;
    my $hd = Test::HandyData::mysql->new(dbh => $dbh);


    #  Write test code here.
    test_nullable($hd);
    test_pk($hd);


    $dbh->disconnect();

    done_testing();
}


#  No values will be assigned to nullable column. 
#  (Although default value will be assigned if exists)
sub test_nullable {
    my ($hd) = @_;

    $hd->fk(1);
    my $dbh = $hd->dbh;

    $dbh->do(q{
        CREATE TABLE test_nullable_foreign (
            id int primary key,
            name varchar(10)
        )
    });
    $dbh->do(q{
        CREATE TABLE test_nullable (
            col_with_default    int DEFAULT 100,
            col_without_default int,
            col_with_fk         int,
            CONSTRAINT FOREIGN KEY (col_with_fk) REFERENCES test_nullable_foreign(id)
        )
    });
    
    my $id = $hd->process_table('test_nullable');
    is($id, undef);

    my ($count) = $dbh->selectrow_array(q{ SELECT COUNT(*) FROM test_nullable });
    is($count, 1);

    ($count) = $dbh->selectrow_array(q{ SELECT COUNT(*) FROM test_nullable_foreign });
    is($count, 0);

    my @cols = $dbh->selectrow_array(q{
        SELECT col_with_default, col_without_default, col_with_fk 
        FROM test_nullable
    });
    is($cols[0], 100);      #  col_with_default (default value will be assigned)
    is($cols[1], undef);    #  col_without_default -> undef
    is($cols[2], undef);    #  col_with_fk -> undef

}


sub test_pk {
    my ($hd) = @_;

    my $dbh = $hd->dbh;

    #  auto_increment primary key
    $dbh->do(q{
        CREATE TABLE test_pk_ai (
            id int primary key auto_increment
        )});
    #  If pk value is specified, the value will be used.
    my $id = $hd->process_table('test_pk_ai', { id => 200 });
    is($id, 200);

    #  If pk value is not specified and pk column is auto_increment,
    #  auto_increment value will be used.
    $dbh->do(q{ALTER TABLE test_pk_ai AUTO_INCREMENT = 300});
    $hd->_set_user_valspec('test_pk_ai', {});   #  reset valspec
    $id = $hd->process_table('test_pk_ai');
    is($id, 300);
    $id = $hd->process_table('test_pk_ai');
    is($id, 301);
    
    #  Non-auto_increment primary key 
    $dbh->do(q{
        CREATE TABLE test_pk_nai (
            id int primary key
        )});
    #  Random value will be assigned.
    $id = $hd->process_table('test_pk_nai');
    like($id, qr/^\d+$/, "(random id is $id)");
   
    
    #  Varchar primary key
    $dbh->do(q{
        CREATE TABLE test_pk_varchar (
            id varchar(10) primary key
        )});
    $id = $hd->process_table('test_pk_varchar');
    like($id, qr/^\w{10}$/, "(random id is $id)");
    $id = $hd->process_table('test_pk_varchar');
    like($id, qr/^\w{10}$/, "(random id is $id)");

    $id = $hd->process_table('test_pk_varchar', { id => 'abcde12345' });
    is($id, 'abcde12345');     
}


sub test_notnull {
    my ($hd) = @_;


}
        
