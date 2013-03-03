#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 9;
use DBI;
use Test::mysqld;
use Test::Exception;


use Test::HandyData::mysql;



main();
exit(0);


=pod

valspec のテスト


=cut

sub main {
    my $hd = Test::HandyData::mysql->new();
    
    #  get
    is_deeply($hd->valspec(), {}, "Initial state (empty hash)");
   
    #  set
    lives_ok { $hd->valspec({ id => 100 }) };
    is_deeply($hd->{_valspec}, { id => 100 }, "Set a valspec");
    is_deeply($hd->valspec(), { id => 100 }, "Get a valspec");
   
    #  override
    lives_ok { $hd->valspec({ name => 'foo' }) };
    is_deeply($hd->valspec(), { name => 'foo' }, "Override"); 

    #  invalid types
    dies_ok { $hd->valspec(1) } 'Try to set a number';
    dies_ok { $hd->valspec('test') } 'Try to set a string';
    dies_ok { $hd->valspec([ 'a', 'b' ]) } 'Type to set an arrayref'; 
}




