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

cond のテスト


=cut

sub main {
    my $hd = Test::HandyData::mysql->new();
    
    #  get
    is_deeply($hd->cond(), {}, "Initial state (empty hash)");
   
    #  set
    lives_ok { $hd->cond({ id => 100 }) };
    is_deeply($hd->{_cond}, { id => 100 }, "Set a condition");
    is_deeply($hd->cond(), { id => 100 }, "Get a condition");
   
    #  override
    lives_ok { $hd->cond({ name => 'foo' }) };
    is_deeply($hd->cond(), { name => 'foo' }, "Override"); 

    #  invalid types
    dies_ok { $hd->cond(1) } 'Try to set a number';
    dies_ok { $hd->cond('test') } 'Try to set a string';
    dies_ok { $hd->cond([ 'a', 'b' ]) } 'Type to set an arrayref'; 
}




