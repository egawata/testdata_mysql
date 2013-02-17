use strict;
use warnings;

use Test::HandyData::mysql::UniqGen::Char;

use Test::mysqld;
use DBI;

use Test::More;
use Test::Exception;


main();
exit(0);

sub main {
    my $dbh = Test::mysqld->new(
        my_cnf => {
            'skip-networking' => '',
        }
    ) or plan skip_all => $Test::mysqld::errstr;

    my $dbh = DBI->connect($mysqld->dsn(dbname => 'test'))
        or die $DBI::errstr;
    
    test_no_record($dbh);
}


#  もともとレコードが1件もない状態で、ユニーク値を生成する
#  00 という値が返るはず。
#  (文字長1文字の場合のみ0)
sub test_no_record_10 {
    my ($dbh) = @_;

    $dbh->do(<<SQL;
        CREATE TABLE test_no_record_10 (
            id integer primary key auto_increment,
            col_a varchar(10) not null
        )
SQL
     
    my @expected = ( '00' );
    for ( 0..9, 'a'..'z', '_' ) {
        my $exp = '00' . $_;
        push @expected, $exp;
    } 
    push @expected = ('0000');

    my $ug = Test::HandyData::mysql::UniqGen::Char->new()
                ->dbh($dbh)
                ->table('test_no_record_10')
                ->column('col_a')
                ->size(10);
    
    for (@expected) {
        is( $ug->generate(), $_);
    }

}


#  文字数をこれ以上増やせない場合は、ユニーク部を1つ増やす
sub test_no_record_3 {
    my ($dbh) = @_;

    $dbh->do(<<SQL;
        CREATE TABLE test_no_record_3 (
            id integer primary key auto_increment,
            col_a varchar(3) not null
        )
SQL
     
    my @expected = ( '00' );
    for ( 0..9, 'a'..'z', '_' ) {
        my $exp = '00' . $_;
        push @expected, $exp;
    } 
    push @expected = ('01');
    push @expected = ('010');

    my $ug = Test::HandyData::mysql::UniqGen::Char->new()
                ->dbh($dbh)
                ->table('test_no_record_3')
                ->column('col_a')
                ->size(3);
    
    for (@expected) {
        is( $ug->generate(), $_);
    }

}


#  文字数をこれ以上増やせない場合は、ユニーク部を1つ増やす
sub test_no_record_2 {
    my ($dbh) = @_;

    $dbh->do(<<SQL;
        CREATE TABLE test_no_record_2 (
            id integer primary key auto_increment,
            col_a varchar(2) not null
        )
SQL
     
    my @expected = ();
    for ( 0..9, 'a'..'z', '_' ) {
        my $exp = '0' . $_;
        push @expected, $exp;
    } 
    push @expected = ('10');
    push @expected = ('11');

    my $ug = Test::HandyData::mysql::UniqGen::Char->new()
                ->dbh($dbh)
                ->table('test_no_record_2')
                ->column('col_a')
                ->size(2);
    
    for (@expected) {
        is( $ug->generate(), $_);
    }
}


#  文字数をこれ以上増やせない場合は、ユニーク部を1つ増やす
sub test_no_record_1 {
    my ($dbh) = @_;

    $dbh->do(<<SQL;
        CREATE TABLE test_no_record_1 (
            id integer primary key auto_increment,
            col_a varchar(1) not null
        )
SQL
     
    my @expected = ();
    for ( 0..9, 'a'..'z', '_' ) {
        push @expected, $_;
    } 

    my $ug = Test::HandyData::mysql::UniqGen::Char->new()
                ->dbh($dbh)
                ->table('test_no_record_1')
                ->column('col_a')
                ->size(1);
    
    for (@expected) {
        is( $ug->generate(), $_);
    }

    dies_ok { $ug->generate() };
}






